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
    dag_id='583_postgresql_mart_update',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt':'2022-05-06'}

###POSTGRESQL settings###
#set postgresql connectionfrom basehook
pg_conn = BaseHook.get_connection('pg_connection')

##init test connection
conn = psycopg2.connect(f"dbname='student' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()


#3. обновление таблиц d по загруженным данным в staging-слой

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


t_update_mart_d_tables = PythonOperator(task_id='update_mart_d_tables',
                                        python_callable=update_mart_d_tables,
                                        dag=dag)


t_update_mart_f_tables = PythonOperator(task_id='update_mart_f_tables',
                                        python_callable=update_mart_f_tables,
                                        dag=dag)


t_update_mart_d_tables >> t_update_mart_f_tables