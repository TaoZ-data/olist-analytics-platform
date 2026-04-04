with source as (
    select * from {{ source('raw', 'RAW_ORDERS') }}
),

renamed as (
    select
        ORDER_ID                                    as order_id,
        CUSTOMER_ID                                 as customer_id,
        ORDER_STATUS                                as order_status,
        to_timestamp_ntz(ORDER_PURCHASE_TIMESTAMP)  as order_purchase_at,
        to_timestamp_ntz(ORDER_APPROVED_AT)         as order_approved_at,
        to_timestamp_ntz(ORDER_DELIVERED_CARRIER_DATE) as order_delivered_carrier_at,
        to_timestamp_ntz(ORDER_DELIVERED_CUSTOMER_DATE) as order_delivered_customer_at,
        to_timestamp_ntz(ORDER_ESTIMATED_DELIVERY_DATE) as order_estimated_delivery_at
    from source
)

select * from renamed