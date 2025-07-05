{{ config(materialized='table') }}

with schedules_data as (
    select
        date(scheduled_departure_date) as flight_date,
        carrier_code,
        departure_airport,
        arrival_airport,
        aircraft_type,
        count(*) as scheduled_flights,
        sum(estimated_co2_total_tonnes) as scheduled_co2_tonnes,
        sum(estimated_fuel_burn_total_tonnes) as scheduled_fuel_tonnes,
        avg(estimated_co2_total_tonnes) as avg_scheduled_co2_per_flight,
        avg(estimated_fuel_burn_total_tonnes) as avg_scheduled_fuel_per_flight
    from {{ ref('stg_estimated_emissions_schedules_sample') }}
    where scheduled_departure_date is not null
    group by 1, 2, 3, 4, 5
),

status_data as (
    select
        date(scheduled_departure_date) as flight_date,
        carrier_code,
        departure_airport,
        arrival_airport,
        aircraft_type,
        count(*) as actual_flights,
        sum(estimated_co2_total_tonnes) as actual_co2_tonnes,
        sum(estimated_fuel_burn_total_tonnes) as actual_fuel_tonnes,
        avg(estimated_co2_total_tonnes) as avg_actual_co2_per_flight,
        avg(estimated_fuel_burn_total_tonnes) as avg_actual_fuel_per_flight,
        sum(case when is_operating = true then 1 else 0 end) as operating_flights
    from {{ ref('stg_estimated_emissions_status_sample') }}
    where scheduled_departure_date is not null
    group by 1, 2, 3, 4, 5
)

select
    coalesce(s.flight_date, st.flight_date) as flight_date,
    coalesce(s.carrier_code, st.carrier_code) as carrier_code,
    coalesce(s.departure_airport, st.departure_airport) as departure_airport,
    coalesce(s.arrival_airport, st.arrival_airport) as arrival_airport,
    coalesce(s.aircraft_type, st.aircraft_type) as aircraft_type,
    
    -- Scheduled metrics
    coalesce(s.scheduled_flights, 0) as scheduled_flights,
    coalesce(s.scheduled_co2_tonnes, 0) as scheduled_co2_tonnes,
    coalesce(s.scheduled_fuel_tonnes, 0) as scheduled_fuel_tonnes,
    s.avg_scheduled_co2_per_flight,
    s.avg_scheduled_fuel_per_flight,
    
    -- Actual metrics
    coalesce(st.actual_flights, 0) as actual_flights,
    coalesce(st.actual_co2_tonnes, 0) as actual_co2_tonnes,
    coalesce(st.actual_fuel_tonnes, 0) as actual_fuel_tonnes,
    st.avg_actual_co2_per_flight,
    st.avg_actual_fuel_per_flight,
    coalesce(st.operating_flights, 0) as operating_flights,
    
    -- Calculated metrics
    case 
        when s.scheduled_flights > 0 then 
            (st.actual_flights::float / s.scheduled_flights::float) * 100 
        else null 
    end as flight_completion_rate_pct,
    
    case 
        when s.scheduled_co2_tonnes > 0 then 
            ((st.actual_co2_tonnes - s.scheduled_co2_tonnes) / s.scheduled_co2_tonnes) * 100 
        else null 
    end as co2_variance_pct,
    
    case 
        when s.scheduled_fuel_tonnes > 0 then 
            ((st.actual_fuel_tonnes - s.scheduled_fuel_tonnes) / s.scheduled_fuel_tonnes) * 100 
        else null 
    end as fuel_variance_pct

from schedules_data s
full outer join status_data st
    on s.flight_date = st.flight_date
    and s.carrier_code = st.carrier_code
    and s.departure_airport = st.departure_airport
    and s.arrival_airport = st.arrival_airport
    and s.aircraft_type = st.aircraft_type 