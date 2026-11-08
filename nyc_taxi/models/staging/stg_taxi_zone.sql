select 
cast(LocationID as integer) as location_id,
Borough as borough,
Zone as zone_name,
service_zone as service_zone
from {{ ref('taxi_zone') }}