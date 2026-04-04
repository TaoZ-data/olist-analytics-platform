with source as (
    select * from {{ source('raw', 'RAW_ORDER_REVIEWS') }}
),

renamed as (
    select
        REVIEW_ID                               as review_id,
        ORDER_ID                                as order_id,
        cast(REVIEW_SCORE as int)               as review_score,
        REVIEW_COMMENT_TITLE                    as review_comment_title,
        REVIEW_COMMENT_MESSAGE                  as review_comment_message,
        to_timestamp_ntz(REVIEW_CREATION_DATE)  as review_created_at,
        to_timestamp_ntz(REVIEW_ANSWER_TIMESTAMP) as review_answered_at
    from source
)

select * from renamed