from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import snowflake.connector
from datetime import datetime, timedelta
import requests
import logging
import json
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API
airflow_api_conn = 'rapidapi_linkedin'
api_url = "https://linkedin-data-api.p.rapidapi.com/search-jobs"
api_host = "linkedin-data-api.p.rapidapi.com"
# querystring Params
keywords = '"data engineer"'
locationId = "103644278"
datePosted = "past24Hours"
jobType = "fullTime"
onsiteRemote = "remote"
sort = "mostRecent"
aws_conn_id = 'aws_default'

# S3
bucket = 'birkbeck-job-search'
s3_folder = 'job_search'
s3_file_name = "linkedin_response.json"

# DB
airflow_pg_conn = 'pg_jobs'


def get_rapidapi_key():
    """Fetch the RapidAPI key from the Airflow connection."""
    try:
        connection = BaseHook.get_connection(airflow_api_conn)
        return connection.extra_dejson.get('headers').get('x-rapidapi-key')
    except Exception as e:
        logger.error("Failed to get RapidAPI key: %s", e)
        raise

def upload_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket using Airflow's S3Hook with partitioning by date."""
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)  # Use the connection stored in Airflow
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
            "keywords": '"data engineer"',
            "locationId": "103644278",
            "datePosted": "past24Hours",
            "jobType": "fullTime",
            "onsiteRemote": "remote",
            "sort": "mostRecent",
        }
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()

        # Save the response to a local file
        local_file_path = f"/tmp/{s3_file_name}"
        with open(local_file_path, "w") as file:
            json.dump(response.json(), file, indent=4)

        # Upload the JSON file to S3
        s3_file_key = f"{s3_folder}/{s3_file_name}"
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
    file_name = s3_file_name
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
    s3_file_key = f"{date_partition}/{s3_folder}/{s3_file_name}"
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

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    transformed_data = []
    for item in data:
        transformed_data.append({
            'id': item.get('id', 'N/A'),
            'title': item.get('title', 'N/A'),
            'url': item.get('url', 'N/A'),
            'company_name': item['company'].get('name', 'N/A'),
            'location': item.get('location', 'N/A'),
            'post_at': item.get('postAt', 'N/A'),
            'posted_timestamp': item.get('postedTimestamp', 'N/A'),
            'benefits': item.get('benefits', 'N/A')
        })
    return transformed_data


def load_data(**kwargs):
    ti = kwargs["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform_data")
    pg_hook = PostgresHook(postgres_conn_id=airflow_pg_conn)
    conn = pg_hook.get_conn()

    cursor = conn.cursor()

    # Create linkedin_jobs table if it doesn't exist
    create_linkedin_table_sql = """
    CREATE TABLE IF NOT EXISTS linkedin_jobs (
        id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255),
        url VARCHAR(255),
        company_name VARCHAR(255),
        location VARCHAR(255),
        post_at TIMESTAMP,
        posted_timestamp BIGINT,
        benefits TEXT
    );
    """
    cursor.execute(create_linkedin_table_sql)

    for item in transformed_data:
        cursor.execute(
            """
            INSERT INTO linkedin_jobs (id, title, url, company_name, location, post_at, posted_timestamp, benefits)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                url = EXCLUDED.url,
                company_name = EXCLUDED.company_name,
                location = EXCLUDED.location,
                post_at = EXCLUDED.post_at,
                posted_timestamp = EXCLUDED.posted_timestamp,
                benefits = EXCLUDED.benefits;
        """,
            (
                item["id"],
                item["title"],
                item["url"],
                item["company_name"],
                item["location"],
                item["post_at"],
                item["post_timestamp"],
                item["benefits"],
            ),
        )
    conn.commit()
    cursor.close()
    conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 24),
    'email': 'dave.birkbeck@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'job_search_linkedin',
    catchup=False,
    default_args=default_args,
    description='A DAG to call job search API, export JSON to S3, then import from S3 into PostgreSQL',
    schedule_interval=timedelta(hours=6)
    ) as dag:

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

    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )

call_api >> extract_data_task >> transform_data_task >> load_data_task