{{
    config(
        materialized = 'view',
    )
}}

SELECT 
    FLIGHT_IDENTIFIER as "Flight Identifier", 
    AIRLINE_CODE as "Airline Code",
    FLIGHT_NUMBER as "Flight Number",
    DEPARTURE_STANDARD_DATETIME as "Departure Standard Datetime",
    DEPARTURE_LOCAL_DATETIME as "Departure Local Datetime",
    INSERTDATETIME as "Insert Datetime"
FROM {{ source('INFLIGHTSALES_DIM_STG', 'FLIGHT_DIM') }}