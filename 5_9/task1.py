import datetime
import time
import psycopg2

import requests
import json
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom

###API settings###
#set api connection from basehook
api_conn = BaseHook.get_connection('create_files_api')
#d5dg1j9kt695d30blp03.apigw.yandexcloud.net
api_endpoint = api_conn.host
#5f55e6c0-e9e5-4a9c-b313-63c01fc31460
api_token = api_conn.password

#set user constants for api
nickname = 'aleksandr-k-vt'
cohort = '1'

#ti.xcom_push(key='report_id', value=report_id)
#report_ids = ti.xcom_pull(key='report_id', task_ids=['check_ready_report'])

headers = {
    "X-API-KEY": api_conn.password,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}


###POSTGRESQL settings###
#set postgresql connectionfrom basehook
psql_conn = BaseHook.get_connection('pg_connection')

##init test connection
conn = psycopg2.connect(f"dbname='student' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()



#запрос выгрузки файлов;
#в итоге вы получите строковый идентификатор задачи выгрузки - task_id;
#через некоторое время по другому пути сформируется ссылка на выгруженные файлы
def create_files_request(ti, api_endpoint , headers):
    method_url = '/generate_report'
    r = requests.post('https://'+api_endpoint + method_url, headers=headers)
    response_dict = json.loads(r.content)
    ti.xcom_push(key='task_id', value=response_dict['task_id'])
    print(f"task_id is {response_dict['task_id']}")
    return response_dict['task_id']



#2. проверка готовности файлов в success
#вы получите строковый идентификатор - это ссылка на файлы, которые можно скачивать
def check_report(ti, api_endpoint , headers):
    task_ids = ti.xcom_pull(key='task_id', task_ids=['create_files_request'])
    task_id = task_ids[0]
    
    method_url = '/get_report'
    payload = {'task_id': task_id}

    #отчёт выгружается 60 секунд минимум;
		#погрузите DAG в сон на 70 секун и сделайте 4 итерации;
		#если отчёт не выгрузился, то должно появится предупреждение об ошибке;
    #если появилась ошибка, значит, что-то не так с выгрузкой по API;
		#чтобы определить ошибку, надо разобраться с генерацией данных;
		#ошибки с выгрузкой встречаются и на практике
    for i in range(4):
        time.sleep(70)
        r = requests.get('https://' + api_endpoint + method_url, params=payload, headers=headers)
        response_dict = json.loads(r.content)
        print(i, response_dict['status'])
        if response_dict['status'] == 'SUCCESS':
            report_id = response_dict['data']['report_id']
            break
    
    #если report_id не объявлен и success не было, то здесь возникнет ошибка
    ti.xcom_push(key='report_id', value=report_id)
    print(f"report_id is {report_id}")
    return report_id




#3. загружаем 3 файла в таблицы stage
def upload_from_s3_to_pg(ti,nickname,cohort):
    report_ids = ti.xcom_pull(key='report_id', task_ids=['check_ready_report'])
    report_id = report_ids[0]

    storage_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT_NUMBER}/{NICKNAME}/{REPORT_ID}/{FILE_NAME}'

    personal_storage_url = storage_url.replace("{COHORT_NUMBER}", cohort)
    personal_storage_url = personal_storage_url.replace("{NICKNAME}", nickname)
    personal_storage_url = personal_storage_url.replace("{REPORT_ID}", report_id)


    #insert to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    #get custom_research
    df_customer_research = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "customer_research.csv") )
    df_customer_research.reset_index(drop = True, inplace = True)
    insert_cr = "insert into stage.customer_research (date_id,category_id,geo_id,sales_qty,sales_amt) VALUES {cr_val};"
    i = 0
    step = int(df_customer_research.shape[0] / 100)
    while i <= df_customer_research.shape[0]:
        print('df_customer_research' , i, end='\r')
        
        cr_val =  str([tuple(x) for x in df_customer_research.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_cr.replace('{cr_val}',cr_val))
        conn.commit()
        
        i += step+1

    #get order log
    df_order_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_order_log.csv") )
    df_order_log.reset_index(drop = True, inplace = True)
    insert_uol = "insert into stage.user_order_log (date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount) VALUES {uol_val};"
    i = 0
    step = int(df_order_log.shape[0] / 100)
    while i <= df_order_log.shape[0]:
        print('df_order_log',i, end='\r')
        
        uol_val =  str([tuple(x) for x in df_order_log.drop(columns = ['id'] , axis = 1).loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_uol.replace('{uol_val}',uol_val))
        conn.commit()
        
        
        i += step+1

    #get activity log
    df_activity_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_activity_log.csv") )
    df_activity_log.reset_index(drop = True, inplace = True)
    insert_ual = "insert into stage.user_activity_log (date_time, action_id, customer_id, quantity) VALUES {ual_val};"
    i = 0
    step = int(df_activity_log.shape[0] / 100)
    while i <= df_activity_log.shape[0]:
        print('df_activity_log',i, end='\r')
        
        if df_activity_log.drop(columns = ['id'] , axis = 1).loc[i:i + step].shape[0] > 0:
            ual_val =  str([tuple(x) for x in df_activity_log.drop(columns = ['id'] , axis = 1).loc[i:i + step].to_numpy()])[1:-1]
            cur.execute(insert_ual.replace('{ual_val}',ual_val))
            conn.commit()
        
        
        i += step+1


    cur.close()
    conn.close()

    
    return 200


#3. обновление таблиц d по загруженным в staging-слой данным

def update_mart_d_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='student' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    #d_calendar
    mart_d_calendar = """ 
    DELETE FROM mart.d_calendar;
                  INSERT INTO mart.d_calendar (date_id, fact_date, day_num, month_num, month_name, year_num)
    WITH all_dates AS (
        SELECT DISTINCT date_time::date AS fact_date
        FROM stage.user_order_log
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY fact_date) AS date_id,
        fact_date,
        EXTRACT(DAY FROM fact_date) AS day_num,
        EXTRACT(MONTH FROM fact_date) AS month_num,
        TO_CHAR(fact_date, 'Month') AS month_name,
        EXTRACT(YEAR FROM fact_date) AS year_num
    FROM all_dates; COMMIT;
                      """

    cur.execute(mart_d_calendar)
    conn.commit()


    #d_customer
    d_customer = """
               DELETE FROM mart.d_customer;
               INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
               SELECT DISTINCT 
               customer_id AS customer_id,
               first_name,
               last_name,
               (SELECT MAX(city_id) FROM stage.user_order_log WHERE customer_id = u.customer_id) AS city_id
               FROM stage.user_order_log u; COMMIT;
            """

    cur.execute(d_customer)
    conn.commit()

    #d_item -- Вставка данных в таблицу d_item
    d_item = """
            DELETE FROM mart.d_item;
            INSERT INTO mart.d_item (item_id, item_name)
            SELECT DISTINCT 
            item_id,
            item_name
            FROM stage.user_order_log;
            COMMIT;
            """
    cur.execute(d_item)
    conn.commit()

    cur.close()
    conn.close()

    return 200

#4. обновление витрин (таблицы f)
def update_mart_f_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect("host='postgres' port='5432' dbname='student' user='student' password='student-de'")
    cur = conn.cursor()

    #f_activity
    f_activity = """ delete from mart.f_activity;
                     INSERT INTO mart.f_activity (activity_id, date_id, click_number)
                     SELECT 
                     action_id AS activity_id,
                     c.date_id,
                     COUNT(*) AS click_number
                     FROM 
                     stage.user_activity_log u
                     JOIN 
                     mart.d_calendar c ON DATE(u.date_time) = c.fact_date
                     GROUP BY 
                     action_id, c.date_id;
                     COMMIT; 
                 """
    cur.execute(f_activity)
    conn.commit()


    #f_daily_sales
    f_daily_sales = """
                     delete from mart.f_daily_sales;
                     INSERT INTO mart.f_daily_sales  (date_id, item_id, customer_id , price , quantity , payment_amount)
                     SELECT 
                     c.date_id,
                     u.item_id,
                     u.customer_id,
                     AVG(u.payment_amount / NULLIF(u.quantity, 0)) AS price,  -- Избегаем деления на ноль
                     SUM(u.quantity) AS quantity,
                     SUM(u.payment_amount) AS payment_amount
                     FROM 
                     stage.user_order_log u
                     JOIN 
                     mart.d_calendar c ON DATE(u.date_time) = c.fact_date
                     GROUP BY 
                     c.date_id, u.item_id, u.customer_id;
                     COMMIT;
                    """

    cur.execute(f_daily_sales)
    conn.commit()

    cur.close()
    conn.close()

    return 200




#объявление DAG
dag = DAG(
    dag_id='591_full_dag',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)


t_file_request = PythonOperator(task_id='create_files_request',
                                        python_callable= create_files_request,
                                        op_kwargs=headers,
                                        dag=dag)


t_check_report = PythonOperator(task_id='check_report',
                                        python_callable= check_report,
                                        op_kwargs=headers,
                                        dag=dag)


t_upload_from_s3_to_pg = PythonOperator(task_id='t_upload_from_s3_to_pg',
                                        python_callable=upload_from_s3_to_pg,
                                        dag=dag)

t_update_mart_d_tables = PythonOperator(task_id='update_mart_d_tables',
                                        python_callable=update_mart_d_tables,
                                        dag=dag)


t_update_mart_f_tables = PythonOperator(task_id='update_mart_f_tables',
                                        python_callable=update_mart_f_tables,
                                        dag=dag)

#t_file_request >> t_check_report >> t_upload_from_s3_to_pg >> t_update_mart_d_tables >> t_update_mart_f_tables

t_file_request >>t_check_report
t_upload_from_s3_to_pg
t_update_mart_d_tables >> t_update_mart_f_tables
