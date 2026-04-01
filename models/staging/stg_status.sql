with source as (
    select * from {{ source('raw_f1_data', 'raw_status') }}
)

select 
    cast(statusId as INT64) as status_id,
    status as status_description
from source