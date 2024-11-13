from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import logging
import json
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API
airflow_api_conn = 'rapidapi_jsearch'
api_url = "https://jsearch.p.rapidapi.com/search"
api_host = "jsearch.p.rapidapi.com"
# querystring Params
query = "database engineer in united states"
page = "1"
num_pages = "10"
date_posted = "month"
remote_jobs_only = "true"
employment_types = "FULLTIME"

# S3
bucket = 'birkbeck-job-search'

# DB
airflow_pg_conn = 'pg_jobs'

# Snowflake
snowflake_conn = 'snowflake_jobs'


def get_rapidapi_key():
    """Fetch the RapidAPI key from the Airflow connection."""
    try:
        connection = BaseHook.get_connection(airflow_api_conn)
        return connection.extra_dejson.get('headers').get('x-rapidapi-key')
    except Exception as e:
        logger.error("Failed to get RapidAPI key: %s", e)
        raise

def get_snowflake_conn():
    """Fetch the Snowflake connection details from the Airflow connection."""
    try:
        connection = BaseHook.get_connection('snowflake_conn')
        return {
            'user': connection.login,
            'password': connection.password,
            'account': connection.host,
            'warehouse': connection.extra_dejson.get('warehouse'),
            'database': connection.schema,
            'schema': connection.extra_dejson.get('schema')
        }
    except Exception as e:
        logger.error("Failed to get Snowflake connection details: %s", e)
        raise

def upload_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket using Airflow's S3Hook with partitioning by date."""
    s3_hook = S3Hook(aws_conn_id='aws_default')  # Use the connection stored in Airflow
    try:
        # Create a partitioned path based on the current date
        date_partition = datetime.now().strftime('%Y/%m/%d')
        partitioned_object_name = f"{date_partition}/{object_name or file_name}"
        
        s3_hook.load_file(filename=file_name, key=partitioned_object_name, bucket_name=bucket, replace=True)
        print(f"File {file_name} uploaded to s3://{bucket}/{partitioned_object_name}")
    except Exception as e:
        print(f"Failed to upload {file_name} to S3: {e}")
        raise

def call_job_search_api():
    """Fetch job search results from the RapidAPI, store it locally and upload to S3."""
    try:
        url = api_url
        headers = {
            "x-rapidapi-key": get_rapidapi_key(),
            "x-rapidapi-host": api_host
        }
        querystring = {
            "query": query,
            "page": page,
            "num_pages": num_pages,
            "date_posted": date_posted,
            "remote_jobs_only": remote_jobs_only,
            "employment_types": employment_types,
        }
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()

        # Save the response to a local file
        local_file_path = "/tmp/job_search_response.json"
        with open(local_file_path, "w") as file:
            json.dump(response.json(), file, indent=4)

        # Upload the JSON file to S3
        s3_file_key = "job_search/job_search_response.json"
        upload_to_s3(local_file_path, bucket, s3_file_key)

        print(f"Response has been uploaded to s3://{bucket}/{s3_file_key}")
    except requests.exceptions.RequestException as e:
        logger.error("HTTP Request failed: %s", e)
        raise

def download_from_s3(bucket, object_name):
    """Download a file from an S3 bucket using Airflow's S3Hook and manually write it to a file."""
    s3_hook = S3Hook(aws_conn_id='aws_default')  # Use the connection stored in Airflow

    # Manually define the file path
    tmp_dir = "/opt/airflow/tmp"
    file_name = "job_search_response.json"
    local_file_path = os.path.join(tmp_dir, file_name)

    # Ensure the directory exists
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir, exist_ok=True)  # Create the directory if it doesn't exist
        print(f"Directory {tmp_dir} created.")

    try:
        # Fetch the S3 object as bytes
        s3_object = s3_hook.get_key(key=object_name, bucket_name=bucket)
        file_data = s3_object.get()['Body'].read()  # Read the S3 object content as bytes
        
        # Write the S3 object data to a local file
        with open(local_file_path, "wb") as f:
            f.write(file_data)
            print(f"File downloaded from s3://{bucket}/{object_name} and written to {local_file_path}")
        
        return local_file_path  # Return the file path for further use

    except Exception as e:
        print(f"Failed to download {object_name} from S3: {e}")
        raise

def extract_data(**context):
    import json
    # Read JSON data from a file
    date_partition = datetime.now().strftime('%Y/%m/%d')
    s3_file_key = f"{date_partition}/job_search/job_search_response.json"
    local_file_path = download_from_s3(bucket, s3_file_key)  # Now this uses the manually defined path
    print(f"File downloaded from s3://{bucket}/{s3_file_key} and written to variable local_file_path: {local_file_path}")

    try:
        with open(local_file_path, 'r') as file:
            data = json.load(file)
        # Push data to XCom
        context['ti'].xcom_push(key='raw_data', value=data)
    except Exception as e:
        # Log the error and re-raise
        print(f"Error in extract_data: {e}")
        raise e

def transform_data(**context):
    import json
    import logging
    from datetime import datetime
    from urllib.parse import urlparse

    logging.info("Pulling raw_data from XCom")
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract_data')
    if raw_data is None:
        logging.error("No data received from extract_data task")
        raise ValueError("No data received from extract_data task")
    logging.info("raw_data successfully retrieved from XCom")

    # Helper functions
    def parse_boolean(value):
        if isinstance(value, str):
            return value.lower() == 'true'
        return bool(value)

    def parse_float(value):
        try:
            if value is None or value == '':
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def parse_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def validate_url(url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
        
    def clean_job_field(value):
        return (value or '').strip()

    # List to store all transformed jobs
    transformed_jobs = []
    transformed_apply_options_list = []

    # Process each job in the data
    for job in raw_data.get('data', []):
        transformed_data = {}

        # Process each job individually
        transformed_data['job_id'] = job.get('job_id', '').strip()
        if not transformed_data['job_id']:
            logging.error("Job ID is missing or empty.")
            continue  # Skip this job or handle accordingly

        logging.info(f"Processing job_id: {transformed_data['job_id']}")

        # 1. Data Type Conversion and Normalization
        transformed_data['employer_name'] = clean_job_field(job.get('employer_name', ''))
        transformed_data['employer_logo'] = job.get('employer_logo')
        transformed_data['employer_website'] = job.get('employer_website')
        transformed_data['employer_company_type'] = clean_job_field(job.get('employer_company_type', ''))
        transformed_data['employer_linkedin'] = job.get('employer_linkedin')

        # Validate URLs
        url_fields = ['employer_logo', 'employer_website', 'employer_linkedin']
        for field in url_fields:
            url = transformed_data.get(field)
            if url and not validate_url(url):
                transformed_data[field] = None  # Invalid URL, set to None

        # Continue processing other fields...
        transformed_data['job_publisher'] = clean_job_field(job.get('job_publisher', ''))
        transformed_data['job_employment_type'] = clean_job_field(job.get('job_employment_type', '')).upper()
        transformed_data['job_title'] = clean_job_field(job.get('job_title', ''))
        transformed_data['job_apply_link'] = job.get('job_apply_link')
        transformed_data['job_apply_is_direct'] = parse_boolean(job.get('job_apply_is_direct'))
        transformed_data['job_apply_quality_score'] = parse_float(job.get('job_apply_quality_score'))
        transformed_data['job_description'] = clean_job_field(job.get('job_description', ''))
        transformed_data['job_is_remote'] = parse_boolean(job.get('job_is_remote'))

        # Convert timestamps
        transformed_data['job_posted_at_timestamp'] = parse_int(job.get('job_posted_at_timestamp'))
        job_posted_at_datetime_str = job.get('job_posted_at_datetime_utc')
        if job_posted_at_datetime_str:
            transformed_data['job_posted_at_datetime_utc'] = datetime.strptime(
                job_posted_at_datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        else:
            transformed_data['job_posted_at_datetime_utc'] = None

        # Location data
        transformed_data['job_city'] = clean_job_field(job.get('job_city', ''))
        transformed_data['job_state'] = clean_job_field(job.get('job_state', ''))
        transformed_data['job_country'] = clean_job_field(job.get('job_country', '')).upper()
        transformed_data['job_latitude'] = parse_float(job.get('job_latitude'))
        transformed_data['job_longitude'] = parse_float(job.get('job_longitude'))

        # Validate geographical data
        lat = transformed_data['job_latitude']
        lon = transformed_data['job_longitude']

        if lat is None or not (-90 <= lat <= 90):
            transformed_data['job_latitude'] = None
        if lon is None or not (-180 <= lon <= 180):
            transformed_data['job_longitude'] = None

        # Handle optional fields
        transformed_data['job_benefits'] = job.get('job_benefits')
        transformed_data['job_google_link'] = job.get('job_google_link')

        # Offer expiration
        job_offer_expiration_datetime_str = job.get('job_offer_expiration_datetime_utc')
        if job_offer_expiration_datetime_str:
            transformed_data['job_offer_expiration_datetime_utc'] = datetime.strptime(
                job_offer_expiration_datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        else:
            transformed_data['job_offer_expiration_datetime_utc'] = None
        transformed_data['job_offer_expiration_timestamp'] = parse_int(job.get('job_offer_expiration_timestamp'))

        # Flatten 'job_required_experience'
        job_required_experience = job.get('job_required_experience', {})
        transformed_data['job_required_experience_no_experience_required'] = parse_boolean(
            job_required_experience.get('no_experience_required'))
        transformed_data['job_required_experience_required_in_months'] = parse_int(
            job_required_experience.get('required_experience_in_months'))
        transformed_data['job_required_experience_experience_mentioned'] = parse_boolean(
            job_required_experience.get('experience_mentioned'))
        transformed_data['job_required_experience_experience_preferred'] = parse_boolean(
            job_required_experience.get('experience_preferred'))

        # Flatten 'job_required_education'
        job_required_education = job.get('job_required_education', {})
        transformed_data['job_required_education_postgraduate_degree'] = parse_boolean(
            job_required_education.get('postgraduate_degree'))
        transformed_data['job_required_education_professional_certification'] = parse_boolean(
            job_required_education.get('professional_certification'))
        transformed_data['job_required_education_high_school'] = parse_boolean(
            job_required_education.get('high_school'))
        transformed_data['job_required_education_associates_degree'] = parse_boolean(
            job_required_education.get('associates_degree'))
        transformed_data['job_required_education_bachelors_degree'] = parse_boolean(
            job_required_education.get('bachelors_degree'))
        transformed_data['job_required_education_degree_mentioned'] = parse_boolean(
            job_required_education.get('degree_mentioned'))
        transformed_data['job_required_education_degree_preferred'] = parse_boolean(
            job_required_education.get('degree_preferred'))
        transformed_data['job_required_education_professional_certification_mentioned'] = parse_boolean(
            job_required_education.get('professional_certification_mentioned'))

        # Other fields
        transformed_data['job_experience_in_place_of_education'] = parse_boolean(
            job.get('job_experience_in_place_of_education'))
        transformed_data['job_min_salary'] = parse_float(job.get('job_min_salary'))
        transformed_data['job_max_salary'] = parse_float(job.get('job_max_salary'))
        transformed_data['job_salary_currency'] = clean_job_field((job.get('job_salary_currency') or '')).upper()
        transformed_data['job_salary_period'] = clean_job_field(job.get('job_salary_period', ''))
        transformed_data['job_highlights'] = json.dumps(job.get('job_highlights'))  # Convert dict to JSON string
        transformed_data['job_job_title'] = clean_job_field(job.get('job_job_title', ''))
        transformed_data['job_posting_language'] = clean_job_field(job.get('job_posting_language', ''))
        transformed_data['job_onet_soc'] = clean_job_field(job.get('job_onet_soc', ''))
        transformed_data['job_onet_job_zone'] = clean_job_field(job.get('job_onet_job_zone', ''))
        transformed_data['job_occupational_categories'] = job.get('job_occupational_categories')
        transformed_data['job_naics_code'] = clean_job_field(job.get('job_naics_code', ''))
        transformed_data['job_naics_name'] = clean_job_field(job.get('job_naics_name', ''))

        # 2. Handling Null and Missing Values
        # Already handled via 'get' method and default values

        # 3. Data Validation
        # Handled during type parsing and specific validations

        # 4. Consistency Checks
        # Salary fields consistency
        min_salary = transformed_data['job_min_salary']
        max_salary = transformed_data['job_max_salary']
        if min_salary and max_salary and min_salary > max_salary:
            # Swap values if min_salary is greater than max_salary
            transformed_data['job_min_salary'], transformed_data['job_max_salary'] = max_salary, min_salary

        # Experience fields consistency
        if transformed_data['job_required_experience_no_experience_required']:
            transformed_data['job_required_experience_required_in_months'] = 0

        # Add the transformed data to the list
        transformed_jobs.append(transformed_data)

        # 5. Process 'apply_options' separately
        apply_options = job.get('apply_options', [])
        for option in apply_options:
            option_data = {
                'job_id': transformed_data['job_id'],
                'publisher': clean_job_field(option.get('publisher')),
                'apply_link': option.get('apply_link'),
                'is_direct': parse_boolean(option.get('is_direct'))
            }
            # Validate 'apply_link' URL
            if option_data['apply_link'] and not validate_url(option_data['apply_link']):
                option_data['apply_link'] = None
            transformed_apply_options_list.append(option_data)

    # Push transformed data to XCom
    context['ti'].xcom_push(key='transformed_data_list', value=transformed_jobs)
    context['ti'].xcom_push(key='transformed_apply_options_list', value=transformed_apply_options_list)

def load_data(**context):
    import logging

    logging.info("Starting load_data task")

    transformed_data_list = context['ti'].xcom_pull(key='transformed_data_list', task_ids='transform_data')
    transformed_apply_options_list = context['ti'].xcom_pull(key='transformed_apply_options_list', task_ids='transform_data')

    try:
        pg_hook = PostgresHook(postgres_conn_id=airflow_pg_conn)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create job_search table if it doesn't exist
        create_job_search_table_sql = """
        CREATE TABLE IF NOT EXISTS job_search (
            id SERIAL,
            job_id VARCHAR(255) PRIMARY KEY,
            employer_name VARCHAR(255),
            employer_logo TEXT,
            employer_website VARCHAR(255),
            employer_company_type VARCHAR(100),
            employer_linkedin VARCHAR(255),
            job_publisher VARCHAR(100),
            job_employment_type VARCHAR(50),
            job_title VARCHAR(255),
            job_apply_link TEXT,
            job_apply_is_direct BOOLEAN,
            job_apply_quality_score FLOAT,
            job_description TEXT,
            job_is_remote BOOLEAN,
            job_posted_at_timestamp BIGINT,
            job_posted_at_datetime_utc TIMESTAMP,
            job_city VARCHAR(100),
            job_state VARCHAR(100),
            job_country VARCHAR(10),
            job_latitude FLOAT,
            job_longitude FLOAT,
            job_benefits TEXT,
            job_google_link TEXT,
            job_offer_expiration_datetime_utc TIMESTAMP,
            job_offer_expiration_timestamp BIGINT,
            job_required_experience_no_experience_required BOOLEAN,
            job_required_experience_required_in_months INT,
            job_required_experience_experience_mentioned BOOLEAN,
            job_required_experience_experience_preferred BOOLEAN,
            job_required_skills TEXT,
            job_required_education_postgraduate_degree BOOLEAN,
            job_required_education_professional_certification BOOLEAN,
            job_required_education_high_school BOOLEAN,
            job_required_education_associates_degree BOOLEAN,
            job_required_education_bachelors_degree BOOLEAN,
            job_required_education_degree_mentioned BOOLEAN,
            job_required_education_degree_preferred BOOLEAN,
            job_required_education_professional_certification_mentioned BOOLEAN,
            job_experience_in_place_of_education BOOLEAN,
            job_min_salary DECIMAL(10,2),
            job_max_salary DECIMAL(10,2),
            job_salary_currency VARCHAR(10),
            job_salary_period VARCHAR(50),
            job_highlights TEXT,
            job_job_title VARCHAR(255),
            job_posting_language VARCHAR(10),
            job_onet_soc VARCHAR(20),
            job_onet_job_zone VARCHAR(20),
            job_occupational_categories TEXT,
            job_naics_code VARCHAR(20),
            job_naics_name VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_job_search_table_sql)

        # Create apply_options table if it doesn't exist
        create_apply_options_table_sql = """
        CREATE TABLE IF NOT EXISTS apply_options (
            job_id VARCHAR(255),
            publisher VARCHAR(100),
            apply_link TEXT,
            is_direct BOOLEAN,
            FOREIGN KEY (job_id) REFERENCES job_search(job_id),
            PRIMARY KEY (job_id, publisher)
        );
        """
        cursor.execute(create_apply_options_table_sql)

        # Insert or Upsert into 'job_search' table
        if transformed_data_list:
            job_columns = transformed_data_list[0].keys()
            job_columns_str = ', '.join(job_columns)
            job_placeholders = ', '.join(['%s'] * len(job_columns))
            
            # Define the columns to update on conflict
            update_columns = [
                'employer_name', 'employer_logo', 'employer_website',
                'employer_company_type', 'employer_linkedin', 'job_publisher',
                'job_employment_type', 'job_title', 'job_apply_link',
                'job_apply_is_direct', 'job_apply_quality_score', 'job_description',
                'job_is_remote', 'job_posted_at_timestamp', 'job_posted_at_datetime_utc',
                'job_city', 'job_state', 'job_country', 'job_latitude',
                'job_longitude', 'job_benefits', 'job_google_link',
                'job_offer_expiration_datetime_utc', 'job_offer_expiration_timestamp',
                'job_required_experience_no_experience_required',
                'job_required_experience_required_in_months',
                'job_required_experience_experience_mentioned',
                'job_required_experience_experience_preferred',
                'job_required_skills', 'job_required_education_postgraduate_degree',
                'job_required_education_professional_certification',
                'job_required_education_high_school',
                'job_required_education_associates_degree',
                'job_required_education_bachelors_degree',
                'job_required_education_degree_mentioned',
                'job_required_education_degree_preferred',
                'job_required_education_professional_certification_mentioned',
                'job_experience_in_place_of_education', 'job_min_salary',
                'job_max_salary', 'job_salary_currency', 'job_salary_period',
                'job_highlights', 'job_job_title', 'job_posting_language',
                'job_onet_soc', 'job_onet_job_zone', 'job_occupational_categories',
                'job_naics_code', 'job_naics_name', 'updated_at'
            ]
            update_set = ', '.join([f"{col}=EXCLUDED.{col}" for col in update_columns])

            job_insert_query = f"""
                INSERT INTO job_search ({job_columns_str})
                VALUES ({job_placeholders})
                ON CONFLICT (job_id) DO UPDATE SET
                {update_set}
            """
            job_values = [tuple(job[col] for col in job_columns) for job in transformed_data_list]
            cursor.executemany(job_insert_query, job_values)

        # Insert or Upsert into 'apply_options' table
        if transformed_apply_options_list:
            apply_columns = transformed_apply_options_list[0].keys()
            apply_columns_str = ', '.join(apply_columns)
            apply_placeholders = ', '.join(['%s'] * len(apply_columns))

            conflict_columns = ['job_id', 'publisher']
            conflict_columns_str = ', '.join(conflict_columns)
            update_columns = [col for col in apply_columns if col not in conflict_columns]

            apply_insert_query = f"""
                INSERT INTO apply_options ({apply_columns_str})
                VALUES ({apply_placeholders})
                ON CONFLICT ({conflict_columns_str}) DO UPDATE SET
                {', '.join([f"{col}=EXCLUDED.{col}" for col in update_columns])}
            """
            apply_values = [tuple(option[col] for col in apply_columns) for option in transformed_apply_options_list]
            cursor.executemany(apply_insert_query, apply_values)

        conn.commit()
        logging.info("Data loaded successfully into the database")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error in load_data: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

def load_to_snowflake(**context):
    import snowflake.connector

    logging.info("Starting upload_to_snowflake task")

    transformed_data_list = context['ti'].xcom_pull(key='transformed_data_list', task_ids='transform_data')
    transformed_apply_options_list = context['ti'].xcom_pull(key='transformed_apply_options_list', task_ids='transform_data')

    try:
        conn = snowflake.connector.connect(
            user='your_username',
            password='your_password',
            account='your_account',
            warehouse='your_warehouse',
            database='your_database',
            schema='your_schema'
        )
        cursor = conn.cursor()

        # Create job_search table if it doesn't exist
        create_job_search_table_sql = """
        CREATE TABLE IF NOT EXISTS job_search (
            id INTEGER AUTOINCREMENT,
            job_id STRING PRIMARY KEY,
            employer_name STRING,
            employer_logo STRING,
            employer_website STRING,
            employer_company_type STRING,
            employer_linkedin STRING,
            job_publisher STRING,
            job_employment_type STRING,
            job_title STRING,
            job_apply_link STRING,
            job_apply_is_direct BOOLEAN,
            job_apply_quality_score FLOAT,
            job_description STRING,
            job_is_remote BOOLEAN,
            job_posted_at_timestamp BIGINT,
            job_posted_at_datetime_utc TIMESTAMP,
            job_city STRING,
            job_state STRING,
            job_country STRING,
            job_latitude FLOAT,
            job_longitude FLOAT,
            job_benefits STRING,
            job_google_link STRING,
            job_offer_expiration_datetime_utc TIMESTAMP,
            job_offer_expiration_timestamp BIGINT,
            job_required_experience_no_experience_required BOOLEAN,
            job_required_experience_required_in_months INTEGER,
            job_required_experience_experience_mentioned BOOLEAN,
            job_required_experience_experience_preferred BOOLEAN,
            job_required_skills STRING,
            job_required_education_postgraduate_degree BOOLEAN,
            job_required_education_professional_certification BOOLEAN,
            job_required_education_high_school BOOLEAN,
            job_required_education_associates_degree BOOLEAN,
            job_required_education_bachelors_degree BOOLEAN,
            job_required_education_degree_mentioned BOOLEAN,
            job_required_education_degree_preferred BOOLEAN,
            job_required_education_professional_certification_mentioned BOOLEAN,
            job_experience_in_place_of_education BOOLEAN,
            job_min_salary FLOAT,
            job_max_salary FLOAT,
            job_salary_currency STRING,
            job_salary_period STRING,
            job_highlights STRING,
            job_job_title STRING,
            job_posting_language STRING,
            job_onet_soc STRING,
            job_onet_job_zone STRING,
            job_occupational_categories STRING,
            job_naics_code STRING,
            job_naics_name STRING,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_job_search_table_sql)

        # Create apply_options table if it doesn't exist
        create_apply_options_table_sql = """
        CREATE TABLE IF NOT EXISTS apply_options (
            job_id STRING,
            publisher STRING,
            apply_link STRING,
            is_direct BOOLEAN,
            PRIMARY KEY (job_id, publisher),
            FOREIGN KEY (job_id) REFERENCES job_search(job_id)
        );
        """
        cursor.execute(create_apply_options_table_sql)

        # Insert or Upsert into 'job_search' table
        if transformed_data_list:
            job_columns = transformed_data_list[0].keys()
            job_columns_str = ', '.join(job_columns)
            job_placeholders = ', '.join(['%s'] * len(job_columns))
            
            # Define the columns to update on conflict
            update_columns = [
                'employer_name', 'employer_logo', 'employer_website',
                'employer_company_type', 'employer_linkedin', 'job_publisher',
                'job_employment_type', 'job_title', 'job_apply_link',
                'job_apply_is_direct', 'job_apply_quality_score', 'job_description',
                'job_is_remote', 'job_posted_at_timestamp', 'job_posted_at_datetime_utc',
                'job_city', 'job_state', 'job_country', 'job_latitude',
                'job_longitude', 'job_benefits', 'job_google_link',
                'job_offer_expiration_datetime_utc', 'job_offer_expiration_timestamp',
                'job_required_experience_no_experience_required',
                'job_required_experience_required_in_months',
                'job_required_experience_experience_mentioned',
                'job_required_experience_experience_preferred',
                'job_required_skills', 'job_required_education_postgraduate_degree',
                'job_required_education_professional_certification',
                'job_required_education_high_school',
                'job_required_education_associates_degree',
                'job_required_education_bachelors_degree',
                'job_required_education_degree_mentioned',
                'job_required_education_degree_preferred',
                'job_required_education_professional_certification_mentioned',
                'job_experience_in_place_of_education', 'job_min_salary',
                'job_max_salary', 'job_salary_currency', 'job_salary_period',
                'job_highlights', 'job_job_title', 'job_posting_language',
                'job_onet_soc', 'job_onet_job_zone', 'job_occupational_categories',
                'job_naics_code', 'job_naics_name', 'updated_at'
            ]
            update_set = ', '.join([f"{col}=EXCLUDED.{col}" for col in update_columns])

            job_insert_query = f"""
                INSERT INTO job_search ({job_columns_str})
                VALUES ({job_placeholders})
                ON CONFLICT (job_id) DO UPDATE SET
                {update_set}
            """
            job_values = [tuple(job[col] for col in job_columns) for job in transformed_data_list]
            cursor.executemany(job_insert_query, job_values)

        # Insert or Upsert into 'apply_options' table
        if transformed_apply_options_list:
            apply_columns = transformed_apply_options_list[0].keys()
            apply_columns_str = ', '.join(apply_columns)
            apply_placeholders = ', '.join(['%s'] * len(apply_columns))

            conflict_columns = ['job_id', 'publisher']
            conflict_columns_str = ', '.join(conflict_columns)
            update_columns = [col for col in apply_columns if col not in conflict_columns]

            apply_insert_query = f"""
                INSERT INTO apply_options ({apply_columns_str})
                VALUES ({apply_placeholders})
                ON CONFLICT ({conflict_columns_str}) DO UPDATE SET
                {', '.join([f"{col}=EXCLUDED.{col}" for col in update_columns])}
            """
            apply_values = [tuple(option[col] for col in apply_columns) for option in transformed_apply_options_list]
            cursor.executemany(apply_insert_query, apply_values)

        conn.commit()
        logging.info("Data loaded successfully into Snowflake")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error in upload_to_snowflake: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 26),
    'email': 'dave.birkbeck@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'import_job_search',
    catchup=False,
    default_args=default_args,
    description='A DAG to call job search API, export JSON to S3, then import job_search_response.json from S3 into PostgreSQL',
    schedule_interval=timedelta(hours=6)
    # schedule_interval=timedelta(hours=1),
)

call_api = PythonOperator(
    task_id='call_job_search_api_task',
    python_callable=call_job_search_api,
    dag=dag,
)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True
)

call_api >> extract >> transform >> load