SELECT id, uniq_id, date_time, city_id, city_name, customer_id, first_name, item_id, item_name, last_name, payment_amount, quantity
FROM stage.user_order_log;

SELECT COUNT(*)
FROM stage.user_order_log;

SELECT id,uniq_id,date_time,action_id,customer_id,quantity
FROM stage.user_activity_log;

SELECT 
--id, 
date_id, category_id, geo_id, sales_qty, sales_amt
FROM stage.customer_research;

truncate table stage.user_order_log;
truncate table stage.user_activity_log;

/*
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

DROP TABLE IF EXISTS stage.customer_research;
CREATE TABLE IF NOT EXISTS stage.customer_research(
   --ID               INTEGER ,
   date_id          TIMESTAMP ,
   category_id      INTEGER,
   geo_id           INTEGER ,
   sales_qty        INTEGER, 
   sales_amt        numeric(14,2)
   --,PRIMARY KEY (ID)
);

*/
