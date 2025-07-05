{{ config(materialized='table') }}

with phase_breakdown as (
    select
        'taxi_out' as flight_phase,
        sum(estimated_co2_taxi_out_tonnes) as total_co2_tonnes,
        sum(estimated_fuel_burn_taxi_out_tonnes) as total_fuel_tonnes,
        avg(estimated_co2_taxi_out_tonnes) as avg_co2_per_flight,
        avg(estimated_fuel_burn_taxi_out_tonnes) as avg_fuel_per_flight,
        count(*) as total_flights
    from {{ ref('stg_estimated_emissions_status_sample') }}
    where estimated_co2_taxi_out_tonnes is not null
    
    union all
    
    select
        'takeoff' as flight_phase,
        sum(estimated_co2_takeoff_tonnes) as total_co2_tonnes,
        sum(estimated_fuel_burn_takeoff_tonnes) as total_fuel_tonnes,
        avg(estimated_co2_takeoff_tonnes) as avg_co2_per_flight,
        avg(estimated_fuel_burn_takeoff_tonnes) as avg_fuel_per_flight,
        count(*) as total_flights
    from {{ ref('stg_estimated_emissions_status_sample') }}
    where estimated_co2_takeoff_tonnes is not null
    
    union all
    
    select
        'climbout' as flight_phase,
        sum(estimated_co2_climbout_tonnes) as total_co2_tonnes,
        sum(estimated_fuel_burn_climbout_tonnes) as total_fuel_tonnes,
        avg(estimated_co2_climbout_tonnes) as avg_co2_per_flight,
        avg(estimated_fuel_burn_climbout_tonnes) as avg_fuel_per_flight,
        count(*) as total_flights
    from {{ ref('stg_estimated_emissions_status_sample') }}
    where estimated_co2_climbout_tonnes is not null
    
    union all
    
    select
        'cruise' as flight_phase,
        sum(estimated_co2_cruise_tonnes) as total_co2_tonnes,
        sum(estimated_fuel_burn_cruise_tonnes) as total_fuel_tonnes,
        avg(estimated_co2_cruise_tonnes) as avg_co2_per_flight,
        avg(estimated_fuel_burn_cruise_tonnes) as avg_fuel_per_flight,
        count(*) as total_flights
    from {{ ref('stg_estimated_emissions_status_sample') }}
    where estimated_co2_cruise_tonnes is not null
    
    union all
    
    select
        'approach' as flight_phase,
        sum(estimated_co2_approach_tonnes) as total_co2_tonnes,
        sum(estimated_fuel_burn_approach_tonnes) as total_fuel_tonnes,
        avg(estimated_co2_approach_tonnes) as avg_co2_per_flight,
        avg(estimated_fuel_burn_approach_tonnes) as avg_fuel_per_flight,
        count(*) as total_flights
    from {{ ref('stg_estimated_emissions_status_sample') }}
    where estimated_co2_approach_tonnes is not null
    
    union all
    
    select
        'taxi_in' as flight_phase,
        sum(estimated_co2_taxi_in_tonnes) as total_co2_tonnes,
        sum(estimated_fuel_burn_taxi_in_tonnes) as total_fuel_tonnes,
        avg(estimated_co2_taxi_in_tonnes) as avg_co2_per_flight,
        avg(estimated_fuel_burn_taxi_in_tonnes) as avg_fuel_per_flight,
        count(*) as total_flights
    from {{ ref('stg_estimated_emissions_status_sample') }}
    where estimated_co2_taxi_in_tonnes is not null
),

total_emissions as (
    select
        sum(total_co2_tonnes) as grand_total_co2,
        sum(total_fuel_tonnes) as grand_total_fuel
    from phase_breakdown
)

select
    pb.flight_phase,
    pb.total_co2_tonnes,
    pb.total_fuel_tonnes,
    pb.avg_co2_per_flight,
    pb.avg_fuel_per_flight,
    pb.total_flights,
    
    -- Percentage of total emissions
    case 
        when te.grand_total_co2 > 0 then 
            (pb.total_co2_tonnes / te.grand_total_co2) * 100 
        else null 
    end as co2_percentage_of_total,
    
    case 
        when te.grand_total_fuel > 0 then 
            (pb.total_fuel_tonnes / te.grand_total_fuel) * 100 
        else null 
    end as fuel_percentage_of_total,
    
    -- Phase efficiency ranking (lower is better)
    row_number() over (order by pb.avg_co2_per_flight asc) as co2_efficiency_rank,
    row_number() over (order by pb.avg_fuel_per_flight asc) as fuel_efficiency_rank

from phase_breakdown pb
cross join total_emissions te
order by pb.flight_phase 