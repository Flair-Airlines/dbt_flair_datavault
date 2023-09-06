{{
    config(
        materialized = 'view',
    )
}}

SELECT 
    AIRLINE_FLIGHT_IDENTIFIER as "Airline Flight Identifier", 
    AIRLINE_CODE as "Airline Code", 
    FLIGHT_NUMBER as "Flight Number",
    INSERTDATETIME as "Insert Datetime"
FROM {{ source('INFLIGHTSALES_DIM_STG', 'AIRLINE_FLIGHT_DIM') }}