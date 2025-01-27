from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

import datetime
import requests

dag = DAG(
    dag_id='534_bash_sleep_operator',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}


task = BashOperator(task_id='create_files_request',
                    bash_command= 'sleep 10s',
                    dag=dag)

task