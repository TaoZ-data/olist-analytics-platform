with orders as (
    select
        customer_unique_id,
        date_trunc('month', order_purchase_at)  as order_month
    from {{ ref('fact_orders') }}
    where order_status = 'delivered'
),

first_order as (
    select
        customer_unique_id,
        min(order_month)                        as cohort_month
    from orders
    group by customer_unique_id
),

order_with_cohort as (
    select
        o.customer_unique_id,
        o.order_month,
        f.cohort_month,
        datediff('month', f.cohort_month, o.order_month) as month_number
    from orders o
    left join first_order f
        on o.customer_unique_id = f.customer_unique_id
),

cohort_size as (
    select
        cohort_month,
        count(distinct customer_unique_id)      as cohort_size
    from first_order
    group by cohort_month
),

retention as (
    select
        o.cohort_month,
        o.month_number,
        count(distinct o.customer_unique_id)    as retained_customers
    from order_with_cohort o
    group by o.cohort_month, o.month_number
)

select
    r.cohort_month,
    r.month_number,
    r.retained_customers,
    cs.cohort_size,
    round(r.retained_customers / cs.cohort_size * 100, 2) as retention_rate
from retention r
left join cohort_size cs on r.cohort_month = cs.cohort_month
order by r.cohort_month, r.month_number