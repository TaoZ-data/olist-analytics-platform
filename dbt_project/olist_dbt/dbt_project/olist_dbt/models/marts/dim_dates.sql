with date_spine as (
    select dateadd('day', seq4(), '2016-01-01'::date) as date_day
    from table(generator(rowcount => 1200))
),

final as (
    select
        date_day,
        date_trunc('month', date_day)       as date_month,
        year(date_day)                      as year,
        month(date_day)                     as month,
        quarter(date_day)                   as quarter,
        dayofweek(date_day)                 as day_of_week,
        date_day = current_date             as is_today
    from date_spine
)

select * from final