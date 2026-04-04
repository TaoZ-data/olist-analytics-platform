with source as (
    select * from {{ source('raw', 'RAW_ORDER_PAYMENTS') }}
),

renamed as (
    select
        ORDER_ID                        as order_id,
        PAYMENT_SEQUENTIAL              as payment_sequential,
        PAYMENT_TYPE                    as payment_type,
        cast(PAYMENT_INSTALLMENTS as int)   as payment_installments,
        cast(PAYMENT_VALUE as float)        as payment_value
    from source
)

select * from renamed
