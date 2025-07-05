{{ config(materialized='table') }}

with date_spine as (
    select
        dateadd(day, seq4(), date_range.start_date) as flight_date
    from (
        select 
            min(date(scheduled_departure_date)) as start_date,
            max(date(scheduled_departure_date)) as end_date
        from {{ ref('stg_estimated_emissions_status_sample') }}
        where scheduled_departure_date is not null
    ) date_range
    cross join table(generator(rowcount => 1000)) -- Adjust rowcount as needed
    where dateadd(day, seq4(), date_range.start_date) <= date_range.end_date
),

daily_aircraft_metrics as (
    select
        date(scheduled_departure_date) as flight_date,
        aircraft_type,
        count(*) as daily_flights,
        sum(estimated_co2_total_tonnes) as daily_co2_tonnes,
        sum(estimated_fuel_burn_total_tonnes) as daily_fuel_tonnes,
        avg(estimated_co2_total_tonnes) as avg_co2_per_flight,
        avg(estimated_fuel_burn_total_tonnes) as avg_fuel_per_flight,
        sum(case when is_operating = true then 1 else 0 end) as daily_operating_flights,
        count(distinct carrier_code) as daily_unique_carriers,
        count(distinct departure_airport) as daily_unique_departure_airports,
        count(distinct arrival_airport) as daily_unique_arrival_airports
    from {{ ref('stg_estimated_emissions_status_sample') }}
    where aircraft_type is not null and scheduled_departure_date is not null
    group by 1, 2
),

overall_aircraft_metrics as (
    select
        aircraft_type,
        count(*) as total_flights,
        sum(estimated_co2_total_tonnes) as total_co2_tonnes,
        sum(estimated_fuel_burn_total_tonnes) as total_fuel_tonnes,
        avg(estimated_co2_total_tonnes) as overall_avg_co2_per_flight,
        avg(estimated_fuel_burn_total_tonnes) as overall_avg_fuel_per_flight,
        sum(case when is_operating = true then 1 else 0 end) as total_operating_flights,
        count(distinct carrier_code) as unique_carriers,
        count(distinct departure_airport) as unique_departure_airports,
        count(distinct arrival_airport) as unique_arrival_airports,
        
        -- Efficiency metrics
        case 
            when count(*) > 0 then 
                (sum(case when is_operating = true then 1 else 0 end)::float / count(*)::float) * 100 
            else null 
        end as operating_rate_pct,
        
        -- Rankings
        row_number() over (order by count(*) desc) as aircraft_popularity_rank,
        row_number() over (order by avg(estimated_co2_total_tonnes) asc) as co2_efficiency_rank,
        row_number() over (order by avg(estimated_fuel_burn_total_tonnes) asc) as fuel_efficiency_rank
    from {{ ref('stg_estimated_emissions_status_sample') }}
    where aircraft_type is not null
    group by 1
)

select
    ds.flight_date,
    oam.aircraft_type,
    
    -- Daily metrics
    coalesce(dam.daily_flights, 0) as daily_flights,
    coalesce(dam.daily_co2_tonnes, 0) as daily_co2_tonnes,
    coalesce(dam.daily_fuel_tonnes, 0) as daily_fuel_tonnes,
    dam.avg_co2_per_flight,
    dam.avg_fuel_per_flight,
    coalesce(dam.daily_operating_flights, 0) as daily_operating_flights,
    dam.daily_unique_carriers,
    dam.daily_unique_departure_airports,
    dam.daily_unique_arrival_airports,
    
    -- Overall metrics
    oam.total_flights,
    oam.total_co2_tonnes,
    oam.total_fuel_tonnes,
    oam.overall_avg_co2_per_flight,
    oam.overall_avg_fuel_per_flight,
    oam.total_operating_flights,
    oam.unique_carriers,
    oam.unique_departure_airports,
    oam.unique_arrival_airports,
    oam.operating_rate_pct,
    oam.aircraft_popularity_rank,
    oam.co2_efficiency_rank,
    oam.fuel_efficiency_rank,
    
    -- Daily efficiency metrics
    case 
        when dam.daily_flights > 0 then 
            (dam.daily_operating_flights::float / dam.daily_flights::float) * 100 
        else null 
    end as daily_operating_rate_pct

from date_spine ds
cross join overall_aircraft_metrics oam
left join daily_aircraft_metrics dam 
    on ds.flight_date = dam.flight_date 
    and oam.aircraft_type = dam.aircraft_type
order by ds.flight_date, oam.aircraft_type 