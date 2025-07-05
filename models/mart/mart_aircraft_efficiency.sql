{{ config(materialized='table') }}

select
    aircraft_type,
    
    -- Aircraft metrics
    count(*) as total_flights,
    sum(estimated_co2_total_tonnes) as total_co2_tonnes,
    sum(estimated_fuel_burn_total_tonnes) as total_fuel_tonnes,
    avg(estimated_co2_total_tonnes) as avg_co2_per_flight,
    avg(estimated_fuel_burn_total_tonnes) as avg_fuel_per_flight,
    sum(case when is_operating = true then 1 else 0 end) as operating_flights,
    
    -- Fleet diversity
    count(distinct carrier_code) as unique_carriers,
    count(distinct departure_airport) as unique_departure_airports,
    count(distinct arrival_airport) as unique_arrival_airports,
    
    -- Efficiency metrics
    case 
        when count(*) > 0 then 
            (sum(case when is_operating = true then 1 else 0 end)::float / count(*)::float) * 100 
        else null 
    end as operating_rate_pct,
    
    -- Aircraft popularity ranking
    row_number() over (order by count(*) desc) as aircraft_popularity_rank,
    
    -- Emissions efficiency ranking (lower is better)
    row_number() over (order by avg(estimated_co2_total_tonnes) asc) as co2_efficiency_rank,
    
    -- Fuel efficiency ranking (lower is better)
    row_number() over (order by avg(estimated_fuel_burn_total_tonnes) asc) as fuel_efficiency_rank

from {{ ref('stg_estimated_emissions_status_sample') }}
where aircraft_type is not null
group by 1 