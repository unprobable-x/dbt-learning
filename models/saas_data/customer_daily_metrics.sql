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

, date_spine as (
  select date
  from unnest(generate_date_array(date '2023-01-01', date '2024-12-31')) as date
)

, customer_dates as (
  select distinct
    customers.customer_id
    , date_spine.date
  
  from customers
  
  cross join date_spine

  where date_spine.date >= customers.acquisition_date
)

, daily_subscription_metrics as (
  select
    customer_dates.customer_id
    , customer_dates.date
    , count(distinct subscriptions.product_id) as num_products
    , array_agg(distinct subscriptions.product_id) as product_ids
    , sum(subscriptions.arr) as total_arr
    , sum(subscriptions.arr)/12 as total_mrr
    -- 90-day comparison metrics
    , lag(count(distinct subscriptions.product_id),90) over (
        partition by customer_dates.customer_id 
        order by customer_dates.date
      ) as num_products_90d_ago
    , lag(sum(subscriptions.arr),90) over (
        partition by customer_dates.customer_id 
        order by customer_dates.date
      ) as total_arr_90d_ago
    , lag(sum(subscriptions.arr)/12, 90) over (
        partition by customer_dates.customer_id 
        order by customer_dates.date
      ) as total_mrr_90d_ago

  from customer_dates

  left join subscriptions
    on customer_dates.customer_id = subscriptions.customer_id
      and customer_dates.date >= date(subscriptions.start_date)
      and (subscriptions.end_date is null or customer_dates.date <= date(subscriptions.end_date))

  group by 1, 2
)

, daily_login_metrics as (
  select
    customer_dates.customer_id
    , customer_dates.date
    , count(login_events.*) as num_logins
    , count(distinct login_events.user_id) as num_unique_users
    , sum(case when login_events.user_type = 'admin' then 1 else 0 end) as admin_logins
    , sum(case when login_events.user_type = 'advanced' then 1 else 0 end) as advanced_logins
    , sum(case when login_events.user_type = 'standard' then 1 else 0 end) as standard_logins
    , count(distinct case when login_events.user_type = 'admin' then login_events.user_id else null end) as admin_unique_users
    , count(distinct case when login_events.user_type = 'advanced' then login_events.user_id else null end) as advanced_unique_users
    , count(distinct case when login_events.user_type = 'standard' then login_events.user_id else null end) as standard_unique_users
    -- Last 3 months metrics
    , sum(case 
        when date(login_events.login_timestamp) between date_sub(customer_dates.date, interval 90 day) and customer_dates.date
        then 1 else 0 end) as num_logins_last_3m
    , count(distinct case 
        when date(login_events.login_timestamp) between date_sub(customer_dates.date, interval 90 day) and customer_dates.date
        then login_events.user_id else null end) as num_unique_users_last_3m
    -- Previous 3 months metrics
    , sum(case 
        when date(login_events.login_timestamp) between date_sub(customer_dates.date, interval 180 day) and date_sub(customer_dates.date, interval 91 day)
        then 1 else 0 end) as num_logins_prev_3m
    , count(distinct case 
        when date(login_events.login_timestamp) between date_sub(customer_dates.date, interval 180 day) and date_sub(customer_dates.date, interval 91 day)
        then login_events.user_id else null end) as num_unique_users_prev_3m

  from customer_dates

  left join login_events
    on customer_dates.customer_id = login_events.customer_id
      and date(login_events.login_timestamp) = customer_dates.date

  group by 1, 2
)

, daily_ticket_metrics as (
  select
    customer_dates.customer_id
    , customer_dates.date
    -- Cumulative total tickets
    , count(*) over (
        partition by customer_dates.customer_id 
        order by customer_dates.date
        rows between unbounded preceding and current row
      ) as total_tickets_to_date
    -- Open tickets as of each date
    , sum(case 
        when date(support_tickets.created_date) <= customer_dates.date 
          and (support_tickets.status != 'closed' or support_tickets.closed_date is null or date(support_tickets.closed_date) > customer_dates.date)
        then 1 else 0 
      end) as open_tickets
    -- Open tickets by category
    , sum(case 
        when date(support_tickets.created_date) <= customer_dates.date 
          and (support_tickets.status != 'closed' or support_tickets.closed_date is null or date(support_tickets.closed_date) > customer_dates.date)
          and support_tickets.ticket_category = 'bug'
        then 1 else 0 
      end) as open_bug_tickets
    , sum(case 
        when date(support_tickets.created_date) <= customer_dates.date 
          and (support_tickets.status != 'closed' or support_tickets.closed_date is null or date(support_tickets.closed_date) > customer_dates.date)
          and support_tickets.ticket_category = 'incident'
        then 1 else 0 
      end) as open_incident_tickets
    , sum(case 
        when date(support_tickets.created_date) <= customer_dates.date 
          and (support_tickets.status != 'closed' or support_tickets.closed_date is null or date(support_tickets.closed_date) > customer_dates.date)
          and support_tickets.ticket_category = 'question'
        then 1 else 0 
      end) as open_question_tickets
    , sum(case 
        when date(support_tickets.created_date) <= customer_dates.date 
          and (support_tickets.status != 'closed' or support_tickets.closed_date is null or date(support_tickets.closed_date) > customer_dates.date)
          and support_tickets.ticket_category = 'feature request'
        then 1 else 0 
      end) as open_feature_request_tickets
    -- Tickets open for 14+ days
    , sum(case 
        when date(support_tickets.created_date) <= customer_dates.date 
          and (support_tickets.status != 'closed' or support_tickets.closed_date is null or date(support_tickets.closed_date) > customer_dates.date)
          and date_diff(customer_dates.date, date(support_tickets.created_date), day) >= 14
        then 1 else 0 
      end) as tickets_open_14plus_days
    -- Average days to resolution for closed tickets
    , avg(case 
        when support_tickets.status = 'closed' 
          and date(support_tickets.closed_date) <= customer_dates.date
        then date_diff(date(support_tickets.closed_date), date(support_tickets.created_date), day)
        else null
      end) as avg_days_to_resolution
  
  from customer_dates

  left join support_tickets
    on customer_dates.customer_id = support_tickets.customer_id
      and date(support_tickets.created_date) <= customer_dates.date

  group by 1, 2
)

, joined as (
  select
    -- Customer and date identifiers
    customer_dates.customer_id
    , customer_dates.date

    -- Subscription metrics
    , coalesce(daily_subscription_metrics.num_products, 0) as num_products
    , coalesce(daily_subscription_metrics.total_arr, 0) as total_arr
    , coalesce(daily_subscription_metrics.total_mrr, 0) as total_mrr
    -- Subscription percentage changes
    , case 
        when coalesce(daily_subscription_metrics.num_products_90d_ago, 0) = 0 then null
        else round(
            (coalesce(daily_subscription_metrics.num_products, 0) - coalesce(daily_subscription_metrics.num_products_90d_ago, 0)) * 100.0 
            / coalesce(daily_subscription_metrics.num_products_90d_ago, 0)
          , 2)
      end as pct_change_products_90d
    , case 
        when coalesce(daily_subscription_metrics.total_arr_90d_ago, 0) = 0 then null
        else round(
            (coalesce(daily_subscription_metrics.total_arr, 0) - coalesce(daily_subscription_metrics.total_arr_90d_ago, 0)) * 100.0 
            / coalesce(daily_subscription_metrics.total_arr_90d_ago, 0)
          , 2)
      end as pct_change_arr_90d
    , case 
        when coalesce(daily_subscription_metrics.total_mrr_90d_ago, 0) = 0 then null
        else round(
            (coalesce(daily_subscription_metrics.total_mrr, 0) - coalesce(daily_subscription_metrics.total_mrr_90d_ago, 0)) * 100.0 
            / coalesce(daily_subscription_metrics.total_mrr_90d_ago, 0)
          , 2)
      end as pct_change_mrr_90d

    -- Login metrics
    , coalesce(daily_login_metrics.num_logins, 0) as num_logins
    , coalesce(daily_login_metrics.num_unique_users, 0) as num_unique_users
    , coalesce(daily_login_metrics.admin_logins, 0) as admin_logins
    , coalesce(daily_login_metrics.advanced_logins, 0) as advanced_logins
    , coalesce(daily_login_metrics.standard_logins, 0) as standard_logins
    , coalesce(daily_login_metrics.admin_unique_users, 0) as admin_unique_users
    , coalesce(daily_login_metrics.advanced_unique_users, 0) as advanced_unique_users
    , coalesce(daily_login_metrics.standard_unique_users, 0) as standard_unique_users

    -- Login trend metrics
    , coalesce(daily_login_metrics.num_logins_last_3m, 0) as num_logins_last_3m
    , coalesce(daily_login_metrics.num_unique_users_last_3m, 0) as num_unique_users_last_3m
    , coalesce(daily_login_metrics.num_logins_prev_3m, 0) as num_logins_prev_3m
    , coalesce(daily_login_metrics.num_unique_users_prev_3m, 0) as num_unique_users_prev_3m
    , case 
        when coalesce(daily_login_metrics.num_logins_prev_3m, 0) = 0 then null
        else round(
            (coalesce(daily_login_metrics.num_logins_last_3m, 0) - coalesce(daily_login_metrics.num_logins_prev_3m, 0)) * 100.0 
            / coalesce(daily_login_metrics.num_logins_prev_3m, 0)
          , 2)
      end as pct_change_logins_3m
    , case 
        when coalesce(daily_login_metrics.num_unique_users_prev_3m, 0) = 0 then null
        else round(
            (coalesce(daily_login_metrics.num_unique_users_last_3m, 0) - coalesce(daily_login_metrics.num_unique_users_prev_3m, 0)) * 100.0 
            / coalesce(daily_login_metrics.num_unique_users_prev_3m, 0)
          , 2)
      end as pct_change_unique_users_3m

    -- Ticket metrics
    , coalesce(daily_ticket_metrics.total_tickets_to_date, 0) as total_tickets_to_date
    , coalesce(daily_ticket_metrics.open_tickets, 0) as open_tickets
    , coalesce(daily_ticket_metrics.open_bug_tickets, 0) as open_bug_tickets
    , coalesce(daily_ticket_metrics.open_incident_tickets, 0) as open_incident_tickets
    , coalesce(daily_ticket_metrics.open_question_tickets, 0) as open_question_tickets
    , coalesce(daily_ticket_metrics.open_feature_request_tickets, 0) as open_feature_request_tickets
    , coalesce(daily_ticket_metrics.tickets_open_14plus_days, 0) as tickets_open_14plus_days
    , round(coalesce(daily_ticket_metrics.avg_days_to_resolution, 0), 1) as avg_days_to_resolution

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
)

, indicators as (
  select
    *
    , case when pct_change_logins_3m < 0 then true else false end as is_logins_declining
    , case when pct_change_unique_users_3m < 0 then true else false end as is_unique_users_declining
    , case when tickets_open_14plus_days > 3 then true else false end as has_high_aging_tickets
    -- Calculate raw customer risk score
    , round(
        coalesce(pct_change_products_90d * -1, 0) + 
        coalesce(pct_change_arr_90d * -1, 0) + 
        coalesce(pct_change_logins_3m * -1, 0) + 
        coalesce(pct_change_unique_users_3m * -1, 0) + 
        -- Ratio of critical tickets (bugs, incidents, long-running) to total open tickets
        case 
          when open_tickets = 0 then 0
          else ((open_bug_tickets + open_incident_tickets + tickets_open_14plus_days) * 100.0 / open_tickets)
        end
      , 2) as customer_risk_score_raw
  
  from joined
)

, final as (
    select 
      *
      -- Normalize risk score across customers for each date (0-100 scale)
      , round(
        case 
          when max(customer_risk_score_raw) over (partition by date) = min(customer_risk_score_raw) over (partition by date) then 50
          else 100.0 * (customer_risk_score_raw - min(customer_risk_score_raw) over (partition by date)) 
            / (max(customer_risk_score_raw) over (partition by date) - min(customer_risk_score_raw) over (partition by date))
        end
      , 2) as customer_risk_score

    from indicators
)

select * from final


