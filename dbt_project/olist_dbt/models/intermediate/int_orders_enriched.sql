with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

order_payments as (
    select
        order_id,
        sum(payment_value)                    as total_payment_value,
        count(distinct payment_sequential)    as payment_count,
        max(payment_type)                     as primary_payment_type,
        max(payment_installments)             as max_installments
    from {{ ref('stg_order_payments') }}
    group by order_id
),

order_items_agg as (
    select
        order_id,
        count(order_item_id)        as total_items,
        sum(price)                  as total_price,
        sum(freight_value)          as total_freight,
        sum(price + freight_value)  as total_order_value
    from {{ ref('stg_order_items') }}
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_at,
        o.order_approved_at,
        o.order_delivered_carrier_at,
        o.order_delivered_customer_at,
        o.order_estimated_delivery_at,

        -- item metrics
        oia.total_items,
        oia.total_price,
        oia.total_freight,
        oia.total_order_value,

        -- payment metrics
        op.total_payment_value,
        op.payment_count,
        op.primary_payment_type,
        op.max_installments,

        -- delivery metrics
        datediff('day',
            o.order_purchase_at,
            o.order_delivered_customer_at)      as actual_delivery_days,
        datediff('day',
            o.order_purchase_at,
            o.order_estimated_delivery_at)      as estimated_delivery_days,
        datediff('day',
            o.order_delivered_customer_at,
            o.order_estimated_delivery_at)      as delivery_delay_days,

        case
            when o.order_delivered_customer_at <= o.order_estimated_delivery_at
            then true
            else false
        end                                     as is_on_time

    from orders o
    left join order_items_agg oia on o.order_id = oia.order_id
    left join order_payments op on o.order_id = op.order_id
)

select * from final