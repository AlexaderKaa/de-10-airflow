from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
 
import datetime
import requests
import pandas as pd
import os
import psycopg2
import psycopg2.extras
 
dag = DAG(
    dag_id='552_postgresql_export_function',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt': '2022-05-06'}
 
def load_file_to_pg(filename, pg_table, conn_args):
    # Чтение CSV файла в DataFrame
    df = pd.read_csv(f"/lessons/5. Реализация ETL в Airflow/4. Extract как подключиться к хранилищу, чтобы получить файл/Задание 2/{filename}", index_col=0)
 
    # Формирование списка колонок для SQL запроса
    cols = ','.join(list(df.columns))
    insert_stmt = f"INSERT INTO stage.{pg_table} ({cols}) VALUES %s"
 
    # Подключение к PostgreSQL
    pg_conn = psycopg2.connect(**conn_args)
    cur = pg_conn.cursor()
 
    # Вставка данных в таблицу
    psycopg2.extras.execute_values(cur, insert_stmt, df.values)
    pg_conn.commit()
 
    # Закрытие соединения
    cur.close()
    pg_conn.close()
 
# Пример использования функции в PythonOperator
t_load_file_to_pg = PythonOperator(
    task_id='load_file_to_pg',
    python_callable=load_file_to_pg,
    op_kwargs={
        'filename': 'customer_research.csv',  # Укажите имя файла
        'pg_table': 'customer_research',  # Укажите имя таблицы
        'conn_args': {
            'host': 'your_host',
            'database': 'your_database',
            'user': 'your_user',
            'password': 'your_password'
        }
    },
    dag=dag
)
 
t_load_file_to_pg