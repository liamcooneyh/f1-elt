{{ config(materialized='table') }}

select 
    season_year,
    race_round,
    constructor_name,
    driver_name,
    points_scored,
    sum(points_scored) over (
        partition by season_year, constructor_name
        order by race_round
    ) as cumulative_points
from {{ ref('fct_race_results') }}

