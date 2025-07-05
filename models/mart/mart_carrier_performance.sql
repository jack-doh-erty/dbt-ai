{{ config(materialized='table') }}

select
    carrier_code,
    
    -- Overall carrier metrics
    count(*) as total_flights,
    sum(estimated_co2_total_tonnes) as total_co2_tonnes,
    sum(estimated_fuel_burn_total_tonnes) as total_fuel_tonnes,
    avg(estimated_co2_total_tonnes) as avg_co2_per_flight,
    avg(estimated_fuel_burn_total_tonnes) as avg_fuel_per_flight,
    sum(case when is_operating = true then 1 else 0 end) as operating_flights,
    count(distinct departure_airport) as unique_departure_airports,
    count(distinct arrival_airport) as unique_arrival_airports,
    count(distinct aircraft_type) as unique_aircraft_types,
    
    -- Efficiency metrics
    case 
        when count(*) > 0 then 
            (sum(case when is_operating = true then 1 else 0 end)::float / count(*)::float) * 100 
        else null 
    end as operating_rate_pct,
    
    -- Route diversity
    case 
        when count(*) > 0 then 
            count(distinct departure_airport)::float / count(*)::float 
        else null 
    end as route_diversity_ratio,
    
    -- Aircraft diversity
    case 
        when count(*) > 0 then 
            count(distinct aircraft_type)::float / count(*)::float 
        else null 
    end as aircraft_diversity_ratio

from {{ ref('stg_estimated_emissions_status_sample') }}
where carrier_code is not null
group by 1 