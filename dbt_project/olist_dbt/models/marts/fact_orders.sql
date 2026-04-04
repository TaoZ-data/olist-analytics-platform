with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

reviews as (
    select
        order_id,
        avg(review_score)   as avg_review_score,
        min(review_score)   as min_review_score,
        count(review_id)    as review_count
    from {{ ref('stg_order_reviews') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_at,
    date_trunc('month', o.order_purchase_at)    as order_month,
    date_trunc('week', o.order_purchase_at)     as order_week,
    dayofweek(o.order_purchase_at)              as order_day_of_week,
    hour(o.order_purchase_at)                   as order_hour,
    o.total_items,
    o.total_price,
    o.total_freight,
    o.total_order_value,
    o.total_payment_value,
    o.primary_payment_type,
    o.max_installments,
    o.actual_delivery_days,
    o.estimated_delivery_days,
    o.delivery_delay_days,
    o.is_on_time,
    r.avg_review_score,
    r.review_count
from orders o
left join customers c on o.customer_id = c.customer_id
left join reviews r on o.order_id = r.order_id