{{ config(
  materialized='table'
  ) 
}}

with customer_daily_metrics as (
  select * from {{ ref('customer_daily_metrics') }}
)

, customers as (
  select * from {{ ref('customers') }}
)

, latest_metrics as (
  select 
    customer_id
    , date
    , num_products
    , total_arr
    , total_mrr
    , pct_change_products_90d
    , pct_change_arr_90d
    , pct_change_mrr_90d
    , num_logins
    , num_unique_users
    , admin_logins
    , admin_unique_users
    , advanced_logins
    , advanced_unique_users
    , standard_logins
    , standard_unique_users
    , num_logins_last_3m
    , num_unique_users_last_3m
    , num_logins_prev_3m
    , num_unique_users_prev_3m
    , pct_change_logins_3m
    , pct_change_unique_users_3m
    , total_tickets_to_date
    , open_tickets
    , open_bug_tickets
    , open_incident_tickets
    , open_question_tickets
    , open_feature_request_tickets
    , tickets_open_14plus_days
    , avg_days_to_resolution
    , is_logins_declining
    , is_unique_users_declining
    , has_high_aging_tickets
    , customer_risk_score_raw
    , customer_risk_score

  from customer_daily_metrics

  qualify row_number() over (partition by customer_id order by date desc) = 1
)

, final as (
  select
    customers.customer_id
    , customers.acquisition_date
    , customers.region
    , customers.industry
    , latest_metrics.date as last_metrics_date
    , latest_metrics.num_products
    , latest_metrics.total_arr
    , latest_metrics.total_mrr
    , latest_metrics.pct_change_products_90d
    , latest_metrics.pct_change_arr_90d
    , latest_metrics.pct_change_mrr_90d
    , latest_metrics.num_logins
    , latest_metrics.num_unique_users
    , latest_metrics.admin_logins
    , latest_metrics.admin_unique_users
    , latest_metrics.advanced_logins
    , latest_metrics.advanced_unique_users
    , latest_metrics.standard_logins
    , latest_metrics.standard_unique_users
    , latest_metrics.num_logins_last_3m
    , latest_metrics.num_unique_users_last_3m
    , latest_metrics.num_logins_prev_3m
    , latest_metrics.num_unique_users_prev_3m
    , latest_metrics.pct_change_logins_3m
    , latest_metrics.pct_change_unique_users_3m
    , latest_metrics.total_tickets_to_date
    , latest_metrics.open_tickets
    , latest_metrics.open_bug_tickets
    , latest_metrics.open_incident_tickets
    , latest_metrics.open_question_tickets
    , latest_metrics.open_feature_request_tickets
    , latest_metrics.tickets_open_14plus_days
    , latest_metrics.avg_days_to_resolution
    , latest_metrics.is_logins_declining
    , latest_metrics.is_unique_users_declining
    , latest_metrics.has_high_aging_tickets
    , latest_metrics.customer_risk_score_raw
    , latest_metrics.customer_risk_score
    -- Calculate days since acquisition
    , date_diff(current_date(), customers.acquisition_date, day) as days_since_acquisition
    -- Calculate days since last activity
    , date_diff(current_date(), latest_metrics.date, day) as days_since_last_activity

  from customers

  left join latest_metrics
    on customers.customer_id = latest_metrics.customer_id
)

select * from final