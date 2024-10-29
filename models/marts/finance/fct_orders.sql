select
    orders.customer_id,
    orders.order_id,
    sum(pay.payment_amount) as amount

from {{ ref('stg_jaffle_shop__orders') }} as orders
left join
    {{ ref('stg_stripe__payments') }} as pay
    on orders.order_id = pay.order_id
group by 1, 2
