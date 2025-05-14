WITH daily_subscription_metrics AS (
  SELECT
    customer_id,
    DATE(start_date) as date,
    COUNT(DISTINCT product_id) as num_products,
    SUM(arr) as total_arr,
    SUM(arr)/12 as mrr
  FROM {{ ref('generate_subscription_data') }}
  WHERE DATE(start_date) <= date
    AND (DATE(end_date) >= date OR end_date IS NULL)
  GROUP BY 1, 2
),

daily_login_metrics AS (
  SELECT
    customer_id,
    DATE(login_timestamp) as date,
    user_type,
    COUNT(*) as num_logins,
    COUNT(DISTINCT user_id) as num_unique_users
  FROM {{ ref('generate_login_events') }}
  GROUP BY 1, 2, 3
),

daily_ticket_metrics AS (
  SELECT
    customer_id,
    DATE(created_date) as date,
    COUNT(*) as total_tickets_to_date,
    SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open_tickets,
    SUM(CASE WHEN status = 'open' AND ticket_category = 'bug' THEN 1 ELSE 0 END) as open_bug_tickets,
    SUM(CASE WHEN status = 'open' AND ticket_category = 'incident' THEN 1 ELSE 0 END) as open_incident_tickets,
    SUM(CASE WHEN status = 'open' AND ticket_category = 'question' THEN 1 ELSE 0 END) as open_question_tickets,
    SUM(CASE WHEN status = 'open' AND ticket_category = 'feature request' THEN 1 ELSE 0 END) as open_feature_request_tickets
  FROM {{ ref('generate_support_tickets') }}
  GROUP BY 1, 2
),

date_spine AS (
  SELECT DISTINCT date
  FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2022-12-31')) as date
),

customer_dates AS (
  SELECT DISTINCT
    c.customer_id,
    d.date
  FROM {{ ref('generate_customer_data') }} c
  CROSS JOIN date_spine d
)

SELECT
  cd.customer_id,
  cd.date,
  COALESCE(sm.num_products, 0) as num_products,
  COALESCE(sm.total_arr, 0) as total_arr,
  COALESCE(sm.mrr, 0) as mrr,
  -- Login metrics by user type
  COALESCE(SUM(CASE WHEN lm.user_type = 'admin' THEN lm.num_logins ELSE 0 END), 0) as admin_logins,
  COALESCE(SUM(CASE WHEN lm.user_type = 'advanced' THEN lm.num_logins ELSE 0 END), 0) as advanced_logins,
  COALESCE(SUM(CASE WHEN lm.user_type = 'standard' THEN lm.num_logins ELSE 0 END), 0) as standard_logins,
  COALESCE(SUM(CASE WHEN lm.user_type = 'admin' THEN lm.num_unique_users ELSE 0 END), 0) as admin_unique_users,
  COALESCE(SUM(CASE WHEN lm.user_type = 'advanced' THEN lm.num_unique_users ELSE 0 END), 0) as advanced_unique_users,
  COALESCE(SUM(CASE WHEN lm.user_type = 'standard' THEN lm.num_unique_users ELSE 0 END), 0) as standard_unique_users,
  -- Ticket metrics
  COALESCE(tm.total_tickets_to_date, 0) as total_tickets_to_date,
  COALESCE(tm.open_tickets, 0) as open_tickets,
  COALESCE(tm.open_bug_tickets, 0) as open_bug_tickets,
  COALESCE(tm.open_incident_tickets, 0) as open_incident_tickets,
  COALESCE(tm.open_question_tickets, 0) as open_question_tickets,
  COALESCE(tm.open_feature_request_tickets, 0) as open_feature_request_tickets
FROM customer_dates cd
LEFT JOIN daily_subscription_metrics sm
  ON cd.customer_id = sm.customer_id
  AND cd.date = sm.date
LEFT JOIN daily_login_metrics lm
  ON cd.customer_id = lm.customer_id
  AND cd.date = lm.date
LEFT JOIN daily_ticket_metrics tm
  ON cd.customer_id = tm.customer_id
  AND cd.date = tm.date
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
ORDER BY cd.date, cd.customer_id; 