with source as (
    select * from {{ source('raw_f1_data', 'raw_drivers') }}
)

select 
    driverId as driver_id,
    cast(permanentNumber as INT64) as permanent_number,
    code as driver_code,
    givenName as first_name,
    familyName as last_name,
    cast(dateOfBirth as DATE) as date_of_birth,
    nationality
from source