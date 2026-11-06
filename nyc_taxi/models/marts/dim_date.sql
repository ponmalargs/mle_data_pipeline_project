with bounds as (
    select
        min(pickup_date) as start_date,
        max(dropoff_date) as end_date
    from {{ ref('int_trips_enriched') }}
),

days as (
    select cast(d as date) as date_day
    from bounds,
         generate_series(start_date, end_date, interval 1 day) as t(d)
)

select
    date_day,
    year(date_day)                      as year,
    quarter(date_day)                   as quarter,
    month(date_day)                     as month,
    monthname(date_day)                 as month_name,
    day(date_day)                       as day_of_month,
    isodow(date_day)                    as day_of_week,       -- 1 = Monday, 7 = Sunday
    dayname(date_day)                   as day_name,
    weekofyear(date_day)                as week_of_year,
    isodow(date_day) in (6, 7)          as is_weekend
from days