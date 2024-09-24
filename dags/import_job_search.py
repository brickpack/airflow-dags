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

airflow_api_conn_id = 'rapidapi_jsearch'
api_url = "https://jsearch.p.rapidapi.com/search"
rapidapi_host = "jsearch.p.rapidapi.com"
postgres_airflow_conn = 'pg_etl'
bucket = 'birkbeck-job-search'


def get_rapidapi_key():
    """Fetch the RapidAPI key from the Airflow connection."""
    try:
        connection = BaseHook.get_connection(airflow_api_conn_id)
        return connection.extra_dejson.get('headers').get('x-rapidapi-key')
    except Exception as e:
        logger.error("Failed to get RapidAPI key: %s", e)
        raise

def upload_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket using Airflow's S3Hook."""
    s3_hook = S3Hook(aws_conn_id='aws_default')  # Use the connection stored in Airflow
    try:
        s3_hook.load_file(filename=file_name, key=object_name or file_name, bucket_name=bucket, replace=True)
        print(f"File {file_name} uploaded to s3://{bucket}/{object_name or file_name}")
    except Exception as e:
        print(f"Failed to upload {file_name} to S3: {e}")
        raise

def call_job_search_api():
    """Fetch job search results from the RapidAPI, store it locally and upload to S3."""
    try:
        url = api_url
        headers = {
            "x-rapidapi-key": get_rapidapi_key(),
            "x-rapidapi-host": rapidapi_host
        }
        querystring = {
            "query": "database engineer in united states",
            "page": "1",
            "num_pages": "10",
            "date_posted": "today",
            "remote_jobs_only": "true",
            "employment_types": "FULLTIME",
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
    s3_file_key = "job_search/job_search_response.json"
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
    import re
    from datetime import datetime
    from urllib.parse import urlparse

    logging.info("Pulling raw_data from XCom")
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract_data')
    if raw_data is None:
        logging.error("No data received from extract_data task")
        raise ValueError("No data received from extract_data task")
    logging.info("raw_data successfully retrieved from XCom")

     # If raw_data contains multiple jobs
    for job in raw_data.get('data', []):
        transformed_data = {}
        # Process each job individually
        transformed_data['job_id'] = job.get('job_id', '').strip()
        if not transformed_data['job_id']:
            logging.error("Job ID is missing or empty.")
            continue  # Skip this job or handle accordingly
    
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

    # 1. Data Type Conversion and Normalization
    transformed_data['job_id'] = raw_data.get('job_id', '').strip()
    transformed_data['employer_name'] = raw_data.get('employer_name', '').strip()
    transformed_data['employer_logo'] = raw_data.get('employer_logo')
    transformed_data['employer_website'] = raw_data.get('employer_website')
    transformed_data['employer_company_type'] = raw_data.get('employer_company_type', '').strip()
    transformed_data['employer_linkedin'] = raw_data.get('employer_linkedin')

    # Validate URLs
    url_fields = ['employer_logo', 'employer_website', 'employer_linkedin']
    for field in url_fields:
        url = transformed_data.get(field)
        if url and not validate_url(url):
            transformed_data[field] = None  # Invalid URL, set to None

    # Continue processing other fields...
    transformed_data['job_publisher'] = raw_data.get('job_publisher', '').strip()
    transformed_data['job_employment_type'] = raw_data.get('job_employment_type', '').strip().upper()
    transformed_data['job_title'] = raw_data.get('job_title', '').strip()
    transformed_data['job_apply_link'] = raw_data.get('job_apply_link')
    transformed_data['job_apply_is_direct'] = parse_boolean(raw_data.get('job_apply_is_direct'))
    transformed_data['job_apply_quality_score'] = parse_float(raw_data.get('job_apply_quality_score'))
    transformed_data['job_description'] = raw_data.get('job_description', '').strip()
    transformed_data['job_is_remote'] = parse_boolean(raw_data.get('job_is_remote'))

    # Convert timestamps
    transformed_data['job_posted_at_timestamp'] = parse_int(raw_data.get('job_posted_at_timestamp'))
    job_posted_at_datetime_str = raw_data.get('job_posted_at_datetime_utc')
    if job_posted_at_datetime_str:
        transformed_data['job_posted_at_datetime_utc'] = datetime.strptime(job_posted_at_datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        transformed_data['job_posted_at_datetime_utc'] = None

    # Location data
    transformed_data['job_city'] = raw_data.get('job_city', '').strip()
    transformed_data['job_state'] = raw_data.get('job_state', '').strip()
    transformed_data['job_country'] = raw_data.get('job_country', '').strip().upper()
    transformed_data['job_latitude'] = parse_float(raw_data.get('job_latitude'))
    transformed_data['job_longitude'] = parse_float(raw_data.get('job_longitude'))

    # Validate geographical data
    lat = transformed_data['job_latitude']
    lon = transformed_data['job_longitude']

    if lat is None or not (-90 <= lat <= 90):
        transformed_data['job_latitude'] = None
    if lon is None or not (-180 <= lon <= 180):
        transformed_data['job_longitude'] = None

    # Handle optional fields
    transformed_data['job_benefits'] = raw_data.get('job_benefits')
    transformed_data['job_google_link'] = raw_data.get('job_google_link')

    # Offer expiration
    job_offer_expiration_datetime_str = raw_data.get('job_offer_expiration_datetime_utc')
    if job_offer_expiration_datetime_str:
        transformed_data['job_offer_expiration_datetime_utc'] = datetime.strptime(job_offer_expiration_datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        transformed_data['job_offer_expiration_datetime_utc'] = None
    transformed_data['job_offer_expiration_timestamp'] = parse_int(raw_data.get('job_offer_expiration_timestamp'))

    # Flatten 'job_required_experience'
    job_required_experience = raw_data.get('job_required_experience', {})
    transformed_data['job_required_experience_no_experience_required'] = parse_boolean(job_required_experience.get('no_experience_required'))
    transformed_data['job_required_experience_required_in_months'] = parse_int(job_required_experience.get('required_experience_in_months'))
    transformed_data['job_required_experience_experience_mentioned'] = parse_boolean(job_required_experience.get('experience_mentioned'))
    transformed_data['job_required_experience_experience_preferred'] = parse_boolean(job_required_experience.get('experience_preferred'))

    # Flatten 'job_required_education'
    job_required_education = raw_data.get('job_required_education', {})
    transformed_data['job_required_education_postgraduate_degree'] = parse_boolean(job_required_education.get('postgraduate_degree'))
    transformed_data['job_required_education_professional_certification'] = parse_boolean(job_required_education.get('professional_certification'))
    transformed_data['job_required_education_high_school'] = parse_boolean(job_required_education.get('high_school'))
    transformed_data['job_required_education_associates_degree'] = parse_boolean(job_required_education.get('associates_degree'))
    transformed_data['job_required_education_bachelors_degree'] = parse_boolean(job_required_education.get('bachelors_degree'))
    transformed_data['job_required_education_degree_mentioned'] = parse_boolean(job_required_education.get('degree_mentioned'))
    transformed_data['job_required_education_degree_preferred'] = parse_boolean(job_required_education.get('degree_preferred'))
    transformed_data['job_required_education_professional_certification_mentioned'] = parse_boolean(job_required_education.get('professional_certification_mentioned'))

    # Other fields
    transformed_data['job_experience_in_place_of_education'] = parse_boolean(raw_data.get('job_experience_in_place_of_education'))
    transformed_data['job_min_salary'] = parse_float(raw_data.get('job_min_salary'))
    transformed_data['job_max_salary'] = parse_float(raw_data.get('job_max_salary'))
    transformed_data['job_salary_currency'] = raw_data.get('job_salary_currency', '').strip().upper()
    transformed_data['job_salary_period'] = raw_data.get('job_salary_period', '').strip()
    transformed_data['job_highlights'] = json.dumps(raw_data.get('job_highlights'))  # Convert dict to JSON string
    transformed_data['job_job_title'] = raw_data.get('job_job_title', '').strip()
    transformed_data['job_posting_language'] = raw_data.get('job_posting_language', '').strip()
    transformed_data['job_onet_soc'] = raw_data.get('job_onet_soc', '').strip()
    transformed_data['job_onet_job_zone'] = raw_data.get('job_onet_job_zone', '').strip()
    transformed_data['job_occupational_categories'] = raw_data.get('job_occupational_categories')
    transformed_data['job_naics_code'] = raw_data.get('job_naics_code', '').strip()
    transformed_data['job_naics_name'] = raw_data.get('job_naics_name', '').strip()

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

    # 5. Process 'apply_options' separately
    apply_options = raw_data.get('apply_options', [])
    transformed_apply_options = []
    for option in apply_options:
        option_data = {
            'job_id': transformed_data['job_id'],
            'publisher': option.get('publisher', '').strip(),
            'apply_link': option.get('apply_link'),
            'is_direct': parse_boolean(option.get('is_direct'))
        }
        # Validate 'apply_link' URL
        if option_data['apply_link'] and not validate_url(option_data['apply_link']):
            option_data['apply_link'] = None
        transformed_apply_options.append(option_data)

    # Push transformed data to XCom
    context['ti'].xcom_push(key='transformed_data', value=transformed_data)
    context['ti'].xcom_push(key='transformed_apply_options', value=transformed_apply_options)


def load_data(**context):
    import logging

    logging.info("Starting load_data task")

    transformed_data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    transformed_apply_options = context['ti'].xcom_pull(key='transformed_apply_options', task_ids='transform_data')

    try:
        pg_hook = PostgresHook(postgres_conn_id=postgres_airflow_conn)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

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
            job_naics_name VARCHAR(255)
        );
        """
        cursor.execute(create_job_search_table_sql)

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

        # Insert into 'job_search' table
        job_columns = ', '.join(transformed_data.keys())
        job_placeholders = ', '.join(['%s'] * len(transformed_data))
        job_insert_query = f"""
            INSERT INTO job_search ({job_columns})
            VALUES ({job_placeholders})
            ON CONFLICT (job_id) DO NOTHING
        """
        cursor.execute(job_insert_query, list(transformed_data.values()))

        # Insert into 'apply_options' table
        if transformed_apply_options:
            apply_columns = list(transformed_apply_options[0].keys())
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

            apply_values = [tuple(option[col] for col in apply_columns) for option in transformed_apply_options]

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


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'import_job_search',
    default_args=default_args,
    description='A DAG to call job search API, export JSON to S3, then import job_search_response.json from S3 into PostgreSQL',
    schedule_interval=timedelta(days=1),
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

# load_json_to_postgres_task = PythonOperator(
#     task_id='load_json_to_postgres',
#     python_callable=load_json_to_postgres,
#     dag=dag,
# )

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True
)

call_api >> extract >> transform >> load