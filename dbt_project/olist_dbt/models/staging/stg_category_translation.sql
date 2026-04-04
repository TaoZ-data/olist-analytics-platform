with source as (
    select * from {{ source('raw', 'RAW_CATEGORY_TRANSLATION') }}
),

renamed as (
    select
        PRODUCT_CATEGORY_NAME           as product_category_name,
        PRODUCT_CATEGORY_NAME_ENGLISH   as product_category_name_english
    from source
)

select * from renamed