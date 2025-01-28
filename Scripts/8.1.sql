-- Очистка таблицы d_calendar для контроля очистки данных во всех таблицах.
DELETE FROM mart.d_calendar;
 
-- Вставка данных в таблицу d_calendar
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
FROM all_dates;
 
-- Очистка таблицы d_customer для контроля очистки данных во всех таблицах.
DELETE FROM mart.d_customer;
 
-- Вставка данных в таблицу d_customer
INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
SELECT DISTINCT 
    customer_id AS customer_id,
    first_name,
    last_name,
    (SELECT MAX(city_id) FROM stage.user_order_log WHERE customer_id = u.customer_id) AS city_id
FROM stage.user_order_log u;
 
-- Очистка таблицы d_item для контроля очистки данных во всех таблицах.
DELETE FROM mart.d_item;
 
-- Вставка данных в таблицу d_item
INSERT INTO mart.d_item (item_id, item_name)
SELECT DISTINCT 
    item_id,
    item_name
FROM stage.user_order_log;

COMMIT;