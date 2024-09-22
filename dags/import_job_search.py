from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import logging
import json


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

# def fetch_job_data(**kwargs):
def fetch_job_data():
    try:
        url = api_url
        headers = {
            "x-rapidapi-key": get_rapidapi_key(),
            "x-rapidapi-host": rapidapi_host
        }
        querystring = {
            "query": "database engineer in united states",
            # "query": "database engineer in united states via linkedin",
            "page": "1",
            "num_pages": "10",
            "date_posted": "today",
            "remote_jobs_only": "true",
            "employment_types": "FULLTIME",
        }
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()  # Raises a HTTPError if the HTTP request returned an unsuccessful status code

        # Write the response to a file
        with open("job_search_response.json", "w") as file:
            json.dump(response.json(), file, indent=4)

        print("Response has been written to job_search_response.json")

        data = response.json()
        return data
    
        # # Save data to XCom for downstream tasks
        # return data['results']
    except requests.exceptions.RequestException as e:
        logger.error("HTTP Request failed: %s", e)
        raise

def load_json_to_postgres():
    # Read the JSON file
    with open('job_search_response.json', 'r') as file:
        data = json.load(file)
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=postgres_airflow_conn)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create table if not exists
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

    # Insert data
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
    'start_date': datetime(2024, 9, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'import_job_search',
    default_args=default_args,
    description='A DAG to import job_search_response.json into PostgreSQL',
    schedule_interval=timedelta(days=1),
)

fetch_jobs_task = PythonOperator(
    task_id='fetch_jobs_data',
    python_callable=fetch_job_data,
    provide_context=True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id=postgres_airflow_conn,
    sql="""
    CREATE TABLE IF NOT EXISTS job_search_responses (
        id SERIAL PRIMARY KEY,
        job_title VARCHAR(255),
        company VARCHAR(255),
        location VARCHAR(255),
        description TEXT,
        date_posted DATE
    );
    """,
    dag=dag,
)

import_data_task = PythonOperator(
    task_id='import_data',
    python_callable=load_json_to_postgres,
    dag=dag,
)

fetch_jobs_task >> create_table_task >> import_data_task
