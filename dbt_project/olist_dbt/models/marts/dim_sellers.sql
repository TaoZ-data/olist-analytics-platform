with sellers as (
    select * from {{ ref('stg_sellers') }}
),

seller_metrics as (
    select
        oi.seller_id,
        count(distinct oi.order_id)     as total_orders,
        sum(oi.price)                   as total_revenue,
        avg(oi.price)                   as avg_price,
        sum(oi.freight_value)           as total_freight,
        count(oi.order_item_id)         as total_items_sold
    from {{ ref('stg_order_items') }} oi
    group by oi.seller_id
),

seller_reviews as (
    select
        oi.seller_id,
        avg(r.review_score)             as avg_review_score,
        count(r.review_id)              as total_reviews
    from {{ ref('stg_order_items') }} oi
    left join {{ ref('stg_order_reviews') }} r on oi.order_id = r.order_id
    group by oi.seller_id
)

select
    s.seller_id,
    s.seller_city,
    s.seller_state,
    sm.total_orders,
    sm.total_revenue,
    sm.avg_price,
    sm.total_freight,
    sm.total_items_sold,
    sr.avg_review_score,
    sr.total_reviews
from sellers s
left join seller_metrics sm on s.seller_id = sm.seller_id
left join seller_reviews sr on s.seller_id = sr.seller_id