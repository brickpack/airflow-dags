from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import logging
import json


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_rapidapi_key():
    try:
        connection = BaseHook.get_connection('rapidapi_linkedin')
        return connection.extra_dejson.get('headers').get('x-rapidapi-key')
    except Exception as e:
        logger.error("Failed to get RapidAPI key: %s", e)
        raise

def call_linkedin_api():
    try:
        url = "https://linkedin-data-api.p.rapidapi.com/"
        headers = {
            "x-rapidapi-key": get_rapidapi_key(),
            "x-rapidapi-host": "linkedin-data-api.p.rapidapi.com"
        }
        querystring = {"username":"dave-birkbeck"}
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()  # Raises a HTTPError if the HTTP request returned an unsuccessful status code
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.error("HTTP Request failed: %s", e)
        raise

# def process_data(ti):
#     try:
#         data = ti.xcom_pull(task_ids='call_linkedin_api_task')
#         if data is None:
#             raise ValueError("No data received from the API call")
#         # Process the data here
#         processed_data = some_processing_function(data)
#         # Continue with processed_data
#         ti.xcom_push(key='processed_data', value=processed_data)
#     except Exception as e:
#         logger.error("Error processing data: %s", e)
#         raise

def write_data_to_json(ti):
    try:
        processed_data = ti.xcom_pull(task_ids='process_data_task')
        if processed_data is None:
            raise ValueError("No processed data found")
        output_path = '/Users/me/airflow/output.json'
        with open(output_path, 'w') as json_file:
            json.dump(processed_data, json_file)
        logger.info("Data successfully written to %s", output_path)
    except Exception as e:
        logger.error("Error writing data to JSON: %s", e)
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'linkedin_api_to_db_with_requests',
    default_args=default_args,
    description='A DAG to call LinkedIn API via RapidAPI using requests and process the data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

call_linkedin_api_task = PythonOperator(
    task_id='call_linkedin_api_task',
    python_callable=call_linkedin_api,
    # get_logs=True,
    dag=dag,
)

# process_data_task = PythonOperator(
#     task_id='process_data_task',
#     python_callable=process_data,
#     provide_context=True,
#     dag=dag,
# )

write_data_to_json_task = PythonOperator(
    task_id='write_data_to_json_task',
    python_callable=write_data_to_json,
    provide_context=True,
    # get_logs=True,
    dag=dag,
)

call_linkedin_api_task >> write_data_to_json_task