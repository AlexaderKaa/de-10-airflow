--f_activity
delete from mart.f_activity;
 
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
 
--f_daily_sales
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