with raw_data as (
    select * from {{ source('raw_f1_data', 'raw_results') }}
),

flattened as (
    select 
        r.season,
        r.round,
        r.raceName as race_name,
        -- Use the underscore name from your screenshot:
        r.Circuit_circuitId as circuit_id, 
        r.date as race_date,
        res.position,
        res.points,
        res.grid,
        res.laps,
        res.status,
        -- Note: If these still error, check the schema of the nested 'Results' RECORD
        res.Driver.driverId as driver_id,
        res.Constructor.constructorId as constructor_id,
        res.FastestLap.Time.time as fastest_lap_time
    from raw_data r,
    unnest(r.Results) as res
)

select 
    cast(season as INT64) as season_year,
    cast(round as INT64) as race_round,
    race_name,
    circuit_id,
    cast(race_date as DATE) as race_date,
    cast(position as INT64) as finish_position,
    cast(points as FLOAT64) as points_scored,
    cast(grid as INT64) as start_position,
    cast(laps as INT64) as laps_completed,
    status,
    driver_id,
    constructor_id,
    fastest_lap_time
from flattened