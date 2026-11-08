select
    pickup_date,
    pickup_borough,
    count(1)                                   as trips,
    round(sum(total_amount), 2)                as revenue,
    round(avg(total_amount), 2)                as avg_fare,
    round(avg(trip_distance), 2)               as avg_distance_mi,
    round(avg(trip_duration_min), 1)           as avg_duration_min,
    round(sum(tip_amount) / nullif(sum(fare_amount), 0) * 100, 1) as tip_pct,
    round(sum(cbd_congestion_fee), 2)          as cbd_fees

from {{ ref('fact_trips') }}
group by 1, 2