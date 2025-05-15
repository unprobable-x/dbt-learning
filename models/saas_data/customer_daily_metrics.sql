{{ config(
  materialized='table'
  ) 
}}

{% set date_spine_start = '2023-01-01' %}
{% set date_spine_end = '2024-12-31' %}

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
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="'" ~ date_spine_start ~ "'",
      end_date="'" ~ date_spine_end ~ "'"
    )
  }}
)

, customer_dates as (
  select distinct
    customers.customer_id
    , date_spine.date_day as date
  
  from customers
  
  cross join date_spine

  where date_spine.date_day >= customers.acquisition_date
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
    {% set metrics_90d = ['num_products', 'total_arr', 'total_mrr'] %}
    {% for metric in metrics_90d %}
    , lag(
        {% if metric == 'num_products' %}
          count(distinct subscriptions.product_id)
        {% else %}
          sum(subscriptions.arr) {% if metric == 'total_mrr' %}/ 12{% endif %}
        {% endif %}
      , 90) over (
        partition by customer_dates.customer_id 
        order by customer_dates.date
      ) as {{ metric }}_90d_ago
    {% endfor %}

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
    , count(login_events.user_id) as num_logins
    , count(distinct login_events.user_id) as num_unique_users
    {% set user_types = ['admin', 'advanced', 'standard'] %}
    {% for user_type in user_types %}
    , sum(case when login_events.user_type = '{{ user_type }}' then 1 else 0 end) as {{ user_type }}_logins
    , count(distinct case when login_events.user_type = '{{ user_type }}' then login_events.user_id else null end) as {{ user_type }}_unique_users
    {% endfor %}
    -- Last 3 months metrics
    {% set metrics_3m = ['num_logins', 'num_unique_users'] %}
    {% for metric in metrics_3m %}
    , sum(case 
        when date(login_events.login_timestamp) between date_sub(customer_dates.date, interval 90 day) and customer_dates.date
        then {% if metric == 'num_logins' %}1{% else %}1{% endif %} else null end) as {{ metric }}_last_3m
    , sum(case 
        when date(login_events.login_timestamp) between date_sub(customer_dates.date, interval 180 day) and date_sub(customer_dates.date, interval 91 day)
        then {% if metric == 'num_logins' %}1{% else %}1{% endif %} else null end) as {{ metric }}_prev_3m
    {% endfor %}

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
    {% set ticket_categories = ['bug', 'incident', 'question', 'feature request'] %}
    {% for category in ticket_categories %}
    , sum(case 
        when date(support_tickets.created_date) <= customer_dates.date 
          and (support_tickets.status != 'closed' or support_tickets.closed_date is null or date(support_tickets.closed_date) > customer_dates.date)
          and support_tickets.ticket_category = '{{ category }}'
        then 1 else 0 
      end) as open_{{ category | replace(' ', '_') }}_tickets
    {% endfor %}
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
    {% for metric in ['num_products', 'total_arr', 'total_mrr'] %}
    , case 
        when coalesce(daily_subscription_metrics.{{ metric }}_90d_ago, 0) = 0 then null
        else round(
            (coalesce(daily_subscription_metrics.{{ metric }}, 0) - coalesce(daily_subscription_metrics.{{ metric }}_90d_ago, 0)) * 100.0 
            / coalesce(daily_subscription_metrics.{{ metric }}_90d_ago, 0)
          , 2)
      end as pct_change_{{ metric | replace('num_', '') | replace('total_', '') }}_90d
    {% endfor %}

    -- Login metrics
    , coalesce(daily_login_metrics.num_logins, 0) as num_logins
    , coalesce(daily_login_metrics.num_unique_users, 0) as num_unique_users
    {% for user_type in user_types %}
    , coalesce(daily_login_metrics.{{ user_type }}_logins, 0) as {{ user_type }}_logins
    , coalesce(daily_login_metrics.{{ user_type }}_unique_users, 0) as {{ user_type }}_unique_users
    {% endfor %}

    -- Login trend metrics
    {% for metric in ['num_logins', 'num_unique_users'] %}
    , coalesce(daily_login_metrics.{{ metric }}_last_3m, 0) as {{ metric }}_last_3m
    , coalesce(daily_login_metrics.{{ metric }}_prev_3m, 0) as {{ metric }}_prev_3m
    , case 
        when coalesce(daily_login_metrics.{{ metric }}_prev_3m, 0) = 0 then null
        else round(
            (coalesce(daily_login_metrics.{{ metric }}_last_3m, 0) - coalesce(daily_login_metrics.{{ metric }}_prev_3m, 0)) * 100.0 
            / coalesce(daily_login_metrics.{{ metric }}_prev_3m, 0)
          , 2)
      end as pct_change_{{ metric | replace('num_', '') }}_3m
    {% endfor %}

    -- Ticket metrics
    , coalesce(daily_ticket_metrics.total_tickets_to_date, 0) as total_tickets_to_date
    , coalesce(daily_ticket_metrics.open_tickets, 0) as open_tickets
    {% for category in ticket_categories %}
    , coalesce(daily_ticket_metrics.open_{{ category | replace(' ', '_') }}_tickets, 0) as open_{{ category | replace(' ', '_') }}_tickets
    {% endfor %}
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
    {% for user_type in user_types %}
    , {{ user_type }}_logins
    , {{ user_type }}_unique_users
    {% endfor %}
    , num_logins_last_3m
    , num_unique_users_last_3m
    , num_logins_prev_3m
    , num_unique_users_prev_3m
    , pct_change_logins_3m
    , pct_change_unique_users_3m
    , total_tickets_to_date
    , open_tickets
    {% for category in ticket_categories %}
    , open_{{ category | replace(' ', '_') }}_tickets
    {% endfor %}
    , tickets_open_14plus_days
    , avg_days_to_resolution
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
    {{ dbt_utils.generate_surrogate_key(
      ['customer_id'
      , 'date']
      ) 
    }} as record_key
    , customer_id
    , date
    , num_products
    , total_arr
    , total_mrr
    , pct_change_products_90d
    , pct_change_arr_90d
    , pct_change_mrr_90d
    , num_logins
    , num_unique_users
    {% for user_type in user_types %}
    , {{ user_type }}_logins
    , {{ user_type }}_unique_users
    {% endfor %}
    , num_logins_last_3m
    , num_unique_users_last_3m
    , num_logins_prev_3m
    , num_unique_users_prev_3m
    , pct_change_logins_3m
    , pct_change_unique_users_3m
    , total_tickets_to_date
    , open_tickets
    {% for category in ticket_categories %}
    , open_{{ category | replace(' ', '_') }}_tickets
    {% endfor %}
    , tickets_open_14plus_days
    , avg_days_to_resolution
    , is_logins_declining
    , is_unique_users_declining
    , has_high_aging_tickets
    , customer_risk_score_raw
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


