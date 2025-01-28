from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

import datetime
import requests
import pandas as pd
import os

dag = DAG(
    dag_id='542_s3_load_example',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}

nickname = 'aleksandr-k-vt' 
cohort = '1' # укажите номер когорты 
report_id = 'TWpBeU5TMHdNUzB4TlZReE9Eb3pNam94T1FsaGJHVnJjMkZ1WkhJdGF5MTJkQT09'  # Идентификатор отчёта

def upload_from_s3(file_names):
    # Папка для сохранения файлов
    save_folder = "/lessons/5. Реализация ETL в Airflow/4. Extract как подключиться к хранилищу, чтобы получить файл/Задание 2/"

    # Убедитесь, что папка для сохранения существует
    os.makedirs(save_folder, exist_ok=True)

    # Загружаем и сохраняем файлы в папку stage
    for file_name in files_name:
        #url = f"https://storage.yandexcloud.net/s3-sprint3/{cohort}/{nickname}/{report_id}/{file_name}"
        url = f"https://storage.yandexcloud.net/s3-sprint3-static/lessons/"
        # Загружаем данные из URL
        try:
            df = pd.read_csv(url)
            # Формируем путь для сохранения файла
            file_path = os.path.join(save_folder, file_name)
            # Сохраняем файл
            df.to_csv(file_path, index=False)
       
            print(f"Файл {file_name} сохранён в {file_path}")
        except Exception as e:
           print(f"Ошибка при обработке файла {file_name}: {e}")


t_upload_from_s3 = PythonOperator(task_id='upload_from_s3',
                                        python_callable=upload_from_s3,
                                        op_kwargs={'file_names' : ['customer_research.csv'
                                                                ,'user_activity_log.csv'
                                                                ,'user_order_log.csv']
                                        },
                                        dag=dag)

t_upload_from_s3 