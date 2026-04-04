with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

customer_metrics as (
    select
        customer_unique_id,
        count(distinct o.order_id)              as total_orders,
        sum(o.total_order_value)                as lifetime_value,
        avg(o.total_order_value)                as avg_order_value,
        min(o.order_purchase_at)                as first_order_at,
        max(o.order_purchase_at)                as last_order_at,
        datediff('day',
            min(o.order_purchase_at),
            max(o.order_purchase_at))           as customer_lifespan_days,
        sum(case when o.order_status = 'delivered'
            then 1 else 0 end)                  as delivered_orders,
        avg(o.actual_delivery_days)             as avg_delivery_days
    from {{ ref('stg_customers') }} c
    left join orders o on c.customer_id = o.customer_id
    group by customer_unique_id
)

select * from customer_metrics