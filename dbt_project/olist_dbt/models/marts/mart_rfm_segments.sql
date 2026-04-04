with orders as (
    select
        customer_unique_id,
        order_purchase_at,
        total_order_value
    from {{ ref('fact_orders') }}
    where order_status = 'delivered'
),

max_date as (
    select max(order_purchase_at) as last_date
    from orders
),

rfm_raw as (
    select
        o.customer_unique_id,
        datediff('day',
            max(o.order_purchase_at),
            md.last_date)                       as recency,
        count(distinct o.order_purchase_at)     as frequency,
        sum(o.total_order_value)                as monetary
    from orders o
    cross join max_date md
    group by o.customer_unique_id, md.last_date
),

rfm_scored as (
    select
        customer_unique_id,
        recency,
        frequency,
        monetary,
        ntile(5) over (order by recency desc)       as r_score,
        ntile(5) over (order by frequency asc)      as f_score,
        ntile(5) over (order by monetary asc)       as m_score
    from rfm_raw
),

final as (
    select
        customer_unique_id,
        recency,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        (r_score + f_score + m_score)               as rfm_total,

        case
            when r_score >= 4 and f_score >= 4
                then 'Champions'
            when r_score >= 3 and f_score >= 3
                then 'Loyal Customers'
            when r_score >= 4 and f_score <= 2
                then 'Recent Customers'
            when r_score <= 2 and f_score >= 3
                then 'At Risk'
            when r_score <= 2 and f_score <= 2
                then 'Lost'
            else 'Potential Loyalists'
        end                                         as rfm_segment

    from rfm_scored
)

select * from final