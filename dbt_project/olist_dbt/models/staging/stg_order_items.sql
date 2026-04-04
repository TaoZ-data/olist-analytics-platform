with source as (
    select * from {{ source('raw', 'RAW_ORDER_ITEMS') }}
),

renamed as (
    select
        ORDER_ID                                as order_id,
        ORDER_ITEM_ID                           as order_item_id,
        PRODUCT_ID                              as product_id,
        SELLER_ID                               as seller_id,
        to_timestamp_ntz(SHIPPING_LIMIT_DATE)   as shipping_limit_at,
        cast(PRICE as float)                    as price,
        cast(FREIGHT_VALUE as float)            as freight_value
    from source
)

select * from renamed
