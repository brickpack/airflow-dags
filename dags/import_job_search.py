import boto3
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
import requests
import logging
import json
# import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

airflow_api_conn_id = 'rapidapi_jsearch'
api_url = "https://jsearch.p.rapidapi.com/search"
rapidapi_host = "jsearch.p.rapidapi.com"
postgres_airflow_conn = 'pg_etl'


def get_rapidapi_key():
    try:
        connection = BaseHook.get_connection(airflow_api_conn_id)
        return connection.extra_dejson.get('headers').get('x-rapidapi-key')
    except Exception as e:
        logger.error("Failed to get RapidAPI key: %s", e)
        raise

def upload_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket."""
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, object_name or file_name)
        print(f"File {file_name} uploaded to bucket {bucket}")
    except Exception as e:
        print(f"Failed to upload {file_name} to S3: {e}")
        raise

def call_job_search_api():
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
        bucket_name = "birkbeck-job-search"
        s3_file_key = "job_search/job_search_response.json"
        upload_to_s3(local_file_path, bucket_name, s3_file_key)

        print(f"Response has been uploaded to s3://{bucket_name}/{s3_file_key}")
    except requests.exceptions.RequestException as e:
        logger.error("HTTP Request failed: %s", e)
        raise

def download_from_s3(bucket, object_name, file_name):
    """Download a file from an S3 bucket."""
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket, object_name, file_name)
        print(f"File {file_name} downloaded from bucket {bucket}")
    except Exception as e:
        print(f"Failed to download {object_name} from S3: {e}")
        raise

def load_json_to_postgres():
    # Download the JSON file from S3
    local_file_path = "/tmp/job_search_response.json"
    bucket_name = "your-s3-bucket"
    s3_file_key = "job_search/job_search_response.json"
    download_from_s3(bucket_name, s3_file_key, local_file_path)

    # Read the JSON file and insert it into PostgreSQL
    with open(local_file_path, 'r') as file:
        data = json.load(file)

    pg_hook = PostgresHook(postgres_conn_id=postgres_airflow_conn)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS job_search_responses (
        id SERIAL PRIMARY KEY,
        job_title VARCHAR(255),
        company VARCHAR(255),
        location VARCHAR(255),
        description TEXT,
        date_posted DATE
    );
    """
    cursor.execute(create_table_sql)

    for job in data['jobs']:
        insert_sql = """
        INSERT INTO job_search_responses (job_title, company, location, description, date_posted)
        VALUES (%s, %s, %s, %s, %s);
        """
        cursor.execute(insert_sql, (
            job['title'],
            job['company'],
            job['location'],
            job['description'],
            job['date_posted']
        ))

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'import_job_search',
    default_args=default_args,
    description='A DAG to import job_search_response.json from S3 into PostgreSQL',
    schedule_interval=timedelta(days=1),
)

call_job_search_api_task = PythonOperator(
    task_id='call_job_search_api_task',
    python_callable=call_job_search_api,
    dag=dag,
)

load_json_to_postgres_task = PythonOperator(
    task_id='load_json_to_postgres',
    python_callable=load_json_to_postgres,
    dag=dag,
)

call_job_search_api_task >> load_json_to_postgres_task