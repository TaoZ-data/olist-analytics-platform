with products as (
    select * from {{ ref('stg_products') }}
),

translation as (
    select * from {{ ref('stg_category_translation') }}
),

product_revenue as (
    select
        product_id,
        sum(total_revenue)      as total_revenue,
        sum(total_units_sold)   as total_units_sold,
        sum(total_orders)       as total_orders,
        avg(avg_price)          as avg_price
    from {{ ref('int_product_revenue') }}
    group by product_id
)

select
    p.product_id,
    p.product_category_name,
    coalesce(t.product_category_name_english,
        p.product_category_name)        as product_category_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    pr.total_revenue,
    pr.total_units_sold,
    pr.total_orders,
    pr.avg_price
from products p
left join translation t
    on p.product_category_name = t.product_category_name
left join product_revenue pr
    on p.product_id = pr.product_id