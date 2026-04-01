with source as (
    select * from {{ source('raw_f1_data', 'raw_constructors') }}
)

select 
    constructorId as constructor_id,
    name as constructor_name,
    nationality as constructor_nationality
from source