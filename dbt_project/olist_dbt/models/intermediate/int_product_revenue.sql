with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, order_status, order_purchase_at
    from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

translation as (
    select * from {{ ref('stg_category_translation') }}
),

final as (
    select
        oi.product_id,
        p.product_category_name,
        coalesce(t.product_category_name_english,
            p.product_category_name)            as product_category_english,
        count(distinct oi.order_id)             as total_orders,
        sum(oi.price)                           as total_revenue,
        avg(oi.price)                           as avg_price,
        sum(oi.freight_value)                   as total_freight,
        count(oi.order_item_id)                 as total_units_sold,
        date_trunc('month', o.order_purchase_at) as order_month
    from order_items oi
    left join orders o on oi.order_id = o.order_id
    left join products p on oi.product_id = p.product_id
    left join translation t
        on p.product_category_name = t.product_category_name
    where o.order_status = 'delivered'
    group by
        oi.product_id,
        p.product_category_name,
        product_category_english,
        order_month
)

select * from final