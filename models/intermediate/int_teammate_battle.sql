{{ config(materialized='table') }}

with base as (
    select * from {{ ref('fct_race_results') }}
)

select 
    a.season_year,
    a.race_name,
    a.race_round,
    a.constructor_name,
    a.driver_name,
    b.driver_name as teammate_name,
    a.finish_position as driver_pos,
    b.finish_position as teammate_pos,
    case when a.finish_position < b.finish_position then 1 else 0 end as beat_teammate
    from base a
    join base b 
        on a.race_name = b.race_name 
        and a.season_year = b.season_year 
        and a.constructor_name = b.constructor_name 
        and a.driver_name != b.driver_name


