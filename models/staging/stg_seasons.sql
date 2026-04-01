with source as (
    select * from {{ source('raw_f1_data', 'raw_seasons') }}
)

select 
    cast(season as INT64) as season_year,
    url as wikipedia_url
from source