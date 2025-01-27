from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
import datetime

dag = DAG(
    dag_id='example_http_operator',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}

task = SimpleHttpOperator(
        task_id= 'fetch_files_from_api',
        http_conn_id= 'create_files_api',
        method='POST',
        data=business_dt,
        dag=dag)