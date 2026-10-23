select
    "LocationID"   as zone_id,
    "Borough"      as borough,
    "Zone"         as zone_name,
    service_zone,
    "LocationID" in (1, 132, 138) as is_airport      -- EWR, JFK, LaGuardia
from {{ ref('taxi_zone_lookup') }}
