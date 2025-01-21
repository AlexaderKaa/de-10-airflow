/*
Если у вас данные в виде целого числа — скорее всего, им подойдет тип INTEGER или BIGINT.
Если данные с плавающей точкой — можно использовать NUMERIC.
Для текста подходит varchar или text.
После изучения типов данных создайте 3 таблицы в схеме stage в базе данных с помощью запроса CREATE TABLE. Для этого удобно использовать DBeaver.
Пример создания таблицы с логом активности пользователей:*/

-- id,uniq_id,date_time,action_id,customer_id,quantity
DROP TABLE IF EXISTS stage.user_activity_log;
CREATE TABLE IF NOT EXISTS stage.user_activity_log(
   ID serial ,
   uniq_id UUID,
   date_time          TIMESTAMP ,
   action_id             BIGINT ,
   customer_id             BIGINT ,
   quantity             BIGINT ,
   PRIMARY KEY (ID)
);

-- id,uniq_id,date_time,city_id,city_name,customer_id,first_name,last_name,item_id,item_name,quantity,payment_amount
DROP TABLE IF EXISTS stage.user_order_log;
CREATE TABLE IF NOT EXISTS stage.user_order_log(
   ID               serial ,
   uniq_id          UUID,
   date_time        TIMESTAMP ,
   city_id          INTEGER,
   city_name        VARCHAR(100),
   customer_id      BIGINT ,
   first_name       VARCHAR(100),
   item_id          INTEGER,
   item_name        VARCHAR(100),
   last_name        VARCHAR(100),
   payment_amount   NUMERIC(14,2),
   quantity         BIGINT,
   PRIMARY KEY (ID)
);

-- date_id,category_id,geo_id,sales_qty,sales_amt
DROP TABLE IF EXISTS stage.customer_research;
CREATE TABLE IF NOT EXISTS stage.customer_research(
   --ID               INTEGER ,
   date_id          TIMESTAMP ,
   category_id      INTEGER,
   geo_id           INTEGER ,
   sales_qty        INTEGER, 
   sales_amt        numeric(14,2)
   ,PRIMARY KEY (ID)
);