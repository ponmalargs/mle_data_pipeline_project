select
    trip_id,

    -- keys to dimensions
    vendor_id,
    pickup_location_id,
    dropoff_location_id,
    payment_type,

    -- time
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    pickup_hour,
    pickup_day_name,
    dropoff_date,

    -- descriptive (denormalised for easy querying)
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    payment_type_desc,
    is_paid,

    -- measures
    passenger_count,
    trip_distance,
    trip_duration_min,
    fare_amount,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    cbd_congestion_fee,
    total_amount,

    -- lineage
    source_file,
    loaded_at

from {{ ref('int_trips_enriched') }}
where is_valid_trip