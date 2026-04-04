with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where order_status = 'delivered'
        and order_delivered_customer_at is not null
),

final as (
    select
        order_id,
        customer_id,
        order_purchase_at,
        order_delivered_customer_at,
        order_estimated_delivery_at,
        actual_delivery_days,
        estimated_delivery_days,
        delivery_delay_days,
        is_on_time,

        case
            when actual_delivery_days <= 7  then '0-7 days'
            when actual_delivery_days <= 14 then '8-14 days'
            when actual_delivery_days <= 21 then '15-21 days'
            when actual_delivery_days <= 30 then '22-30 days'
            else '30+ days'
        end                                 as delivery_time_bucket,

        case
            when delivery_delay_days >= 0   then 'on_time'
            when delivery_delay_days >= -3  then 'slightly_late'
            when delivery_delay_days >= -7  then 'moderately_late'
            else 'severely_late'
        end                                 as delay_category

    from orders
)

select * from final