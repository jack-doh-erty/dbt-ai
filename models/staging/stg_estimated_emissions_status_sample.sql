{{ config(materialized='view') }}

select
    carrier_code,
    service_suffix,
    flight_number,
    departure_airport,
    arrival_airport,
    scheduled_departure_date,
    aircraft_type,
    aircraft_registration_number,
    estimated_fuel_burn_taxi_out_tonnes,
    estimated_fuel_burn_takeoff_tonnes,
    estimated_fuel_burn_climbout_tonnes,
    estimated_fuel_burn_cruise_tonnes,
    estimated_fuel_burn_approach_tonnes,
    estimated_fuel_burn_taxi_in_tonnes,
    estimated_fuel_burn_total_tonnes,
    estimated_co2_taxi_out_tonnes,
    estimated_co2_takeoff_tonnes,
    estimated_co2_climbout_tonnes,
    estimated_co2_cruise_tonnes,
    estimated_co2_approach_tonnes,
    estimated_co2_taxi_in_tonnes,
    estimated_co2_total_tonnes,
    missing_actual_times,
    is_operating
from {{ source('oag_flight_emissions_data_sample', 'estimated_emissions_status_sample') }} 