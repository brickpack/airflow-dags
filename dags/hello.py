from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 15),
    'email': ['brickpack@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
   'retries': 0,
   'retry_delay': timedelta(minutes=5),
   'catchup': False,
}

dag = DAG('hello_world', 
          default_args=default_args, 
          schedule_interval=timedelta(days=1)
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    dag=dag
)

t3 = BashOperator(
    task_id='print_bye',
    bash_command='echo "Bye World!"',
    dag=dag
)

t1 >> t2 >> t3
