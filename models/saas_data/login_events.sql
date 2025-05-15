{{ config(
  materialized='table'
  ) 
}}

with login_events as (
  select
    'user' || lpad(cast(floor(rand() * 2000) + 1 as string), 6, '0') as user_id
    , 'cust' || lpad(cast(floor(rand() * 1000) + 1 as string), 6, '0') as customer_id
    , case 
        when rand() < 0.1 then 'admin'
        when rand() < 0.4 then 'advanced'
        else 'standard'
      end as user_type
    , timestamp_add(
        timestamp '2023-01-01 00:00:00'
        , interval cast(floor(rand() * 1000000) as int64) minute
      ) as login_timestamp
  from unnest(generate_array(1, 5000)) as t
)
select * from login_events