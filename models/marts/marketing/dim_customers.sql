with customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

fct_orders as (

    select * from {{ ref('fct_orders') }}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

customer_ltv as (

    select
        customer_id,
        sum(amount) as lifetime_value

    from fct_orders
    group by 1
),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0)
            as number_of_orders,
        coalesce(customer_ltv.lifetime_value, 0) as lifetime_value

    from customers

    left join
        customer_orders
        on customers.customer_id = customer_orders.customer_id
    left join customer_ltv on customers.customer_id = customer_ltv.customer_id

)

select * from final
