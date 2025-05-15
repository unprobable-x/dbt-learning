{{ config(
  materialized='table'
  ) 
}}

with customer_data as (
  select
    'cust' || lpad(cast(row_number() over () as string), 6, '0') as customer_id
    , date_add(date '2023-01-01', interval cast(floor(rand() * 1000) as int64) day) as acquisition_date
    , case 
        when rand() < 0.25 then 'east'
        when rand() < 0.5 then 'west'
        when rand() < 0.75 then 'north'
        else 'south'
      end as region
    , case 
        when rand() < 0.33 then 'technology'
        when rand() < 0.66 then 'banking'
        else 'retail'
      end as industry
  from unnest(generate_array(1, 1000)) as t
)

select * from customer_data