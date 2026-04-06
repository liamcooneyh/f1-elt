{{ config(materialized='table') }}

select * from {{ ref('int_teammate_battle') }}