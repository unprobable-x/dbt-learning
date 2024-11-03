{% set statuses = ['placed', 'shipped', 'returned', 'completed', 'return_pending'] -%}

with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

pivoted as (
select 
customer_id,
order_date,
{%- for state in statuses -%}
COUNT(DISTINCT CASE WHEN status = '{{ state }}' THEN order_id END) as {{ state }}_orders
{%- if not loop.last -%} 
, 
{% endif -%} 
{%- endfor %}


from orders
group by 1,2
)

select * from pivoted

