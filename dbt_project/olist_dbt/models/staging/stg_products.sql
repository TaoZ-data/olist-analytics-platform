with source as (
    select * from {{ source('raw', 'RAW_PRODUCTS') }}
),

renamed as (
    select
        PRODUCT_ID                          as product_id,
        PRODUCT_CATEGORY_NAME               as product_category_name,
        cast(PRODUCT_NAME_LENGHT as int)    as product_name_length,
        cast(PRODUCT_DESCRIPTION_LENGHT as int) as product_description_length,
        cast(PRODUCT_PHOTOS_QTY as int)     as product_photos_qty,
        cast(PRODUCT_WEIGHT_G as float)     as product_weight_g,
        cast(PRODUCT_LENGTH_CM as float)    as product_length_cm,
        cast(PRODUCT_HEIGHT_CM as float)    as product_height_cm,
        cast(PRODUCT_WIDTH_CM as float)     as product_width_cm
    from source
)

select * from renamed