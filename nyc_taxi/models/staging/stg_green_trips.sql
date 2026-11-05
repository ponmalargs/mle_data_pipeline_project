with source as (
    select * from {{ source('clean', 'green_trips') }}
)

select
    md5(concat_ws('|',
        vendor_id, pickup_datetime, dropoff_datetime,
        pickup_location_id, dropoff_location_id, total_amount
    ))                                                      as trip_id,

    -- ids
    vendor_id,
    ratecode_id,
    pickup_location_id,
    dropoff_location_id,

    -- timestamps
    pickup_datetime,
    dropoff_datetime,
    date_diff('minute', pickup_datetime, dropoff_datetime)  as trip_duration_min,

    -- trip info
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,

    -- payment
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    congestion_surcharge,
    cbd_congestion_fee,
    total_amount,

    -- lineage
    source_file,
    loaded_at
from source