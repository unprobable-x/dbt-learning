{{ config(
  materialized='table'
  ) 
}}

with support_tickets as (
  select
    'ticket' || lpad(cast(row_number() over () as string), 6, '0') as ticket_id
    , 'cust' || lpad(cast(floor(rand() * 1000) + 1 as string), 6, '0') as customer_id
    , case 
        when rand() < 0.2 then 'open'
        when rand() < 0.3 then 'on-hold'
        else 'closed'
      end as status
    , timestamp_add(
        timestamp '2023-01-01 00:00:00'
        , interval cast(floor(rand() * 1000000) as int64) minute
      ) as created_date
    , case 
        when rand() < 0.3 then 'bug'
        when rand() < 0.5 then 'incident'
        when rand() < 0.8 then 'question'
        else 'feature request'
      end as ticket_category
  from unnest(generate_array(1, 3000)) as t
)

, final as (
select 
  *
  , case 
      when status = 'closed' then 
        timestamp_add(
          created_date
          , interval cast(floor(rand() * 10080) + 60 as int64) minute 
        )
      else null
    end as closed_date
from support_tickets
)

select * from final
