{{ config(
  materialized='table'
  ) 
}}

with customers as (
  select * from {{ ref('customers') }}
)

, subscriptions as (
  select * from {{ ref('subscriptions') }}
)

, login_events as (
  select * from {{ ref('login_events') }}
)

, support_tickets as (
  select * from {{ ref('support_tickets') }}
)

, daily_subscription_metrics as (
    customer_id
    , date(start_date) as date
    , count(distinct product_id) as num_products
    , array_agg(distinct product_id) as product_ids
    , sum(arr) as total_arr
    , sum(arr)/12 as mrr

  from subscriptions

  group by 1, 2
)

, daily_login_metrics as (
  select
    customer_id
    , date(login_timestamp) as date
    , count(*) as num_logins
    , count(distinct user_id) as num_unique_users
    , sum(case when user_type = 'admin' then 1 else 0 end) as admin_logins
    , sum(case when user_type = 'advanced' then 1 else 0 end) as advanced_logins
    , sum(case when user_type = 'standard' then 1 else 0 end) as standard_logins
    , count(distinct case when user_type = 'admin' then user_id else null end) as admin_unique_users
    , count(distinct case when user_type = 'advanced' then user_id else null end) as advanced_unique_users
    , count(distinct case when user_type = 'standard' then user_id else null end) as standard_unique_users

  from login_events

  group by 1, 2
)

, daily_ticket_metrics as (
  select
    customer_id
    , date(created_date) as date
    , count(*) as total_tickets_to_date
    , sum(case when status = 'open' then 1 else 0 end) as open_tickets
    , sum(case when status = 'open' and ticket_category = 'bug' then 1 else 0 end) as open_bug_tickets
    , sum(case when status = 'open' and ticket_category = 'incident' then 1 else 0 end) as open_incident_tickets
    , sum(case when status = 'open' and ticket_category = 'question' then 1 else 0 end) as open_question_tickets
    , sum(case when status = 'open' and ticket_category = 'feature request' then 1 else 0 end) as open_feature_request_tickets
  
  from support_tickets
  
  group by 1, 2
)

, date_spine as (
  select date
  from unnest(generate_date_array(date '2023-01-01', date '2022-12-31')) as date
)

customer_dates as (
  select distinct
    customers.customer_id
    , date_spine.date
  
  from customers
  
  cross join date_spine

  where date_spine.date >= customers.acquisition_date
)

select
  customer_dates.customer_id
  , customer_dates.date
  , coalesce(daily_subscription_metrics.num_products, 0) as num_products
  , coalesce(daily_subscription_metrics.total_arr, 0) as total_arr
  , coalesce(daily_subscription_metrics.mrr, 0) as mrr
  , coalesce(daily_login_metrics.num_logins, 0) as num_logins
  , coalesce(daily_login_metrics.num_unique_users, 0) as num_unique_users
  , coalesce(daily_login_metrics.admin_logins, 0) as admin_logins
  , coalesce(daily_login_metrics.advanced_logins, 0) as advanced_logins
  , coalesce(daily_login_metrics.standard_logins, 0) as standard_logins
  , coalesce(daily_login_metrics.admin_unique_users, 0) as admin_unique_users
  , coalesce(daily_login_metrics.advanced_unique_users, 0) as advanced_unique_users
  , coalesce(daily_ticket_metrics.total_tickets_to_date, 0) as total_tickets_to_date
  , coalesce(daily_ticket_metrics.open_tickets, 0) as open_tickets
  , coalesce(daily_ticket_metrics.open_bug_tickets, 0) as open_bug_tickets
  , coalesce(daily_ticket_metrics.open_incident_tickets, 0) as open_incident_tickets
  , coalesce(daily_ticket_metrics.open_question_tickets, 0) as open_question_tickets
  , coalesce(daily_ticket_metrics.open_feature_request_tickets, 0) as open_feature_request_tickets

from customer_dates

left join daily_subscription_metrics
  on customer_dates.customer_id = daily_subscription_metrics.customer_id
    and customer_dates.date = daily_subscription_metrics.date

left join daily_login_metrics
  on customer_dates.customer_id = daily_login_metrics.customer_id
    and customer_dates.date = daily_login_metrics.date

left join daily_ticket_metrics
  on customer_dates.customer_id = daily_ticket_metrics.customer_id
    and customer_dates.date = daily_ticket_metrics.date

