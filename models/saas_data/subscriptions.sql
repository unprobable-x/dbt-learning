{{ config(
  materialized='table'
  ) 
}}

with subscription_data as (
  select
    'cust' || lpad(cast(floor(rand() * 1000) + 1 as string), 6, '0') as customer_id
    , 'prod' || lpad(cast(floor(rand() * 50) + 1 as string), 4, '0') as product_id
    , floor(rand() * 5) + 1 as quantity
    , date_add(date '2023-01-01', interval cast(floor(rand() * 1000) as int64) day) as start_date
    , date_add(
        date_add(date '2023-01-01', interval cast(floor(rand() * 1000) as int64) day)
        , interval cast(floor(rand() * 730) as int64) + 30 day
      ) as end_date
    , case 
        when floor(rand() * 3) = 0 then 999
        when floor(rand() * 3) = 1 then 1999
        else 4999
      end as arr
  from unnest(generate_array(1, 2000)) as t
)

select * from subscription_data
where end_date > start_date
