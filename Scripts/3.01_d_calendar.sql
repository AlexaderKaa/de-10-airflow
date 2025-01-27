DROP TABLE IF EXISTS stage.d_calendar;
CREATE TABLE IF NOT EXISTS stage.d_calendar
AS 

--INSERT INTO mart.d_calendar (date_id, fact_date, day_num, month_num, month_name, year_num)
SELECT ROW_NUMBER() OVER (ORDER BY date) AS date_id,
       date::date AS fact_date, -- этого поля нет в структуре вложения в урок, но есть в базе
       EXTRACT(DAY FROM date) AS day_num,
       EXTRACT(MONTH FROM date) AS month_num,
       LEFT(TRIM(to_char(date, 'Month')), 3) AS month_name,  -- 3 буквы названия месяца Тут в базе у меня ругалось на длину, поэтому сделала так
       EXTRACT(YEAR FROM date) AS year_num
FROM generate_series('2020-01-01'::date, '2022-01-01'::date, '1 day'::interval) AS t(date);

COMMIT;

INSERT INTO mart.d_calendar (date_id, fact_date, day_num, month_num, month_name, year_num)
SELECT ROW_NUMBER() OVER (ORDER BY date) AS date_id,
       date::date AS fact_date, -- этого поля нет в структуре вложения в урок, но есть в базе
       EXTRACT(DAY FROM date) AS day_num,
       EXTRACT(MONTH FROM date) AS month_num,
       LEFT(TRIM(to_char(date, 'Month')), 3) AS month_name,  -- 3 буквы названия месяца Тут в базе у меня ругалось на длину, поэтому сделала так
       EXTRACT(YEAR FROM date) AS year_num
FROM generate_series('2020-01-01'::date, '2022-01-01'::date, '1 day'::interval) AS t(date);

COMMIT;


DROP TABLE IF EXISTS stage.d_city;
CREATE TABLE IF NOT EXISTS stage.d_city
AS 
SELECT 
DISTINCT city_id, city_name
FROM stage.user_order_log
WHERE city_id IS NOT NULL AND city_name IS NOT NULL
;


INSERT INTO mart.d_city (city_id, city_name)
SELECT DISTINCT city_id, city_name
FROM stage.user_order_log
WHERE city_id IS NOT NULL AND city_name IS NOT NULL
;


COMMIT;

/*
SELECT date_id, fact_date, day_num, month_num, month_name, year_num
FROM mart.d_calendar;

*/