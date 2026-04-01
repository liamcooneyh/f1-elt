with source as (
    select * from {{ source('raw_f1_data', 'raw_circuits') }}
)

select 
    circuitId as circuit_id,
    circuitName as circuit_name,
    Location_locality as location,
    Location_country as country,
    cast(Location_lat as FLOAT64) as latitude,
    cast(Location_long as FLOAT64) as longitude
from source