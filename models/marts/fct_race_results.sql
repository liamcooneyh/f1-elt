with results as (
    select * from {{ ref('stg_results') }}
),

drivers as (
    select * from {{ ref('stg_drivers') }}
),

constructors as (
    select * from {{ ref('stg_constructors') }}
),

status as (
    select * from {{ ref('stg_status') }}
),

circuits as (
    select * from {{ ref('stg_circuits') }}
),

final_join as (
    select
        -- Race & Time
        r.season_year,
        r.race_round,
        r.race_name,
        r.race_date,
        
        -- Driver Info
        d.first_name,
        d.last_name,
        concat(d.first_name, ' ', d.last_name) as driver_name,
        d.driver_code,
        d.nationality as driver_nationality,
        
        -- Team Info
        c.constructor_name,
        c.constructor_nationality,

        -- Location / Map Info
        ci.circuit_name,
        ci.location as city,
        ci.country,
        ci.latitude,
        ci.longitude,
        
        -- Result Info
        r.finish_position,
        r.points_scored,
        r.start_position,
        r.laps_completed,
        s.status_description,
        r.fastest_lap_time

    from results r
    left join drivers d 
        on r.driver_id = d.driver_id
    left join constructors c 
        on r.constructor_id = c.constructor_id
    left join status s 
        on r.status = s.status_description
    left join circuits ci 
        on r.circuit_id = ci.circuit_id
)

select * from final_join