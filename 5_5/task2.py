from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

import datetime
import requests
import pandas as pd
import os
import psycopg2, psycopg2.extras

dag = DAG(
    dag_id='552_postgresql_export_fuction',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}




def load_file_to_pg(filename, pg_table, conn_args):

    #df = pd.read_csv(f"/lessons/5. Реализация ETL в Airflow/4. Extract как подключиться к хранилищу, чтобы получить файл/Задание 2/{filename}", index_col=0 )#filename = 'customer_research.csv'
    df = pd.read_csv(f"{filename}")
            
    cols = ','.join(list(df.columns))
    
    insert_stmt = f"INSERT INTO stage.{pg_table} ({cols}) VALUES %s"
    

    pg_conn = psycopg2.connect("host='postgres' port='5432' dbname='student' user='student' password='student-de'")
    cur = pg_conn.cursor()

    psycopg2.extras.execute_values(cur, insert_stmt, df.values)
    pg_conn.commit()

    cur.close()
    pg_conn.close()