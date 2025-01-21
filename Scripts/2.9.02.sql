/*В production-слое должны оказаться следующие поля:
Создайте схему prod и таблицы с указанными полями в схеме production-слоя и напишите SQL-запрос миграции из staging в прод-слой полной репликацией.
*/

/* Создание схемы prod */
CREATE SCHEMA prod;

/*
Таблица customer_research:
поле Date_id
поле Geo_id
поле Sales_qty
поле Sales_amt
 */
DROP TABLE IF EXISTS prod.user_order_log;
CREATE TABLE IF NOT EXISTS prod.customer_research
AS 
SELECT 
date_id, category_id, geo_id, sales_qty, sales_amt
FROM stage.customer_research;


/*Таблица user_activity_log:
поле Date_time
поле Customer_id
*/

DROP TABLE IF EXISTS prod.user_activity_log;
CREATE TABLE IF NOT EXISTS prod.user_activity_log
AS 
SELECT 
date_time, customer_id
FROM stage.user_activity_log;

/*
Таблица user_order_log:
поле Date_time
поле Customer_id
поле Quantity
поле Payment_amount
*/

DROP TABLE IF EXISTS prod.user_order_log;
CREATE TABLE IF NOT EXISTS prod.user_order_log
AS 
SELECT 
date_time, customer_id, Quantity, Payment_amount
FROM stage.user_order_log;
