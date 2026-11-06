with trips as (
    select * from {{ ref('stg_green_trips') }}
),

payment_types as (
    select * from {{ ref('stg_payment_types') }}
),

zones as (
    select * from {{ ref('stg_taxi_zone') }}
)

select
    -- everything from staging
    t.*,

    -- payment lookup
    coalesce(p.payment_type_desc, 'Unknown code')  as payment_type_desc,
    p.is_paid,

    -- zone lookups (same table joined twice)
    pu.borough                                     as pickup_borough,
    pu.zone_name                                   as pickup_zone,
    dof.borough                                     as dropoff_borough,
    dof.zone_name                                   as dropoff_zone,

    -- time parts for reporting
    cast(t.pickup_datetime as date)                as pickup_date,
    hour(t.pickup_datetime)                        as pickup_hour,
    dayname(t.pickup_datetime)                     as pickup_day_name,
    cast(t.dropoff_datetime as date)               as dropoff_date,

    -- business-rule flag
    (
        t.trip_distance > 0
        and t.total_amount >= 0
       -- and t.trip_duration_min between 1 and 300
    )                                              as is_valid_trip

from trips t
left join payment_types p on t.payment_type        = p.payment_type
left join zones pu        on t.pickup_location_id  = pu.location_id
left join zones dof        on t.dropoff_location_id = dof.location_id