with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_metrics as (
    select * from {{ ref('int_customer_orders') }}
),

-- Keep only the most recent city/state per customer_unique_id
deduped_customers as (
    select distinct
        customer_unique_id,
        first_value(customer_city) over (
            partition by customer_unique_id
            order by customer_unique_id
        ) as customer_city,
        first_value(customer_state) over (
            partition by customer_unique_id
            order by customer_unique_id
        ) as customer_state
    from customers
),

final as (
    select
        dc.customer_unique_id,
        dc.customer_city,
        dc.customer_state,
        cm.total_orders,
        cm.lifetime_value,
        cm.avg_order_value,
        cm.first_order_at,
        cm.last_order_at,
        cm.customer_lifespan_days,
        cm.delivered_orders,
        cm.avg_delivery_days,

        case
            when cm.total_orders = 1    then 'one_time'
            when cm.total_orders <= 3   then 'occasional'
            else 'loyal'
        end                             as customer_segment,

        case
            when cm.lifetime_value >= 1000  then 'high_value'
            when cm.lifetime_value >= 300   then 'mid_value'
            else 'low_value'
        end                             as value_segment

    from deduped_customers dc
    inner join customer_metrics cm
        on dc.customer_unique_id = cm.customer_unique_id
)

select * from final