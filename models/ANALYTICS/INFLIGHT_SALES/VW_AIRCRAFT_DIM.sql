{{
    config(
        materialized = 'view',
    )
}}

SELECT 
    AIRCRAFT_IDENTIFIER as "Aircraft Identifier", 
    TAILNUMBER as "Tail Number", 
    INSERTDATETIME as "Insert Datetime"
FROM {{ source('INFLIGHTSALES_DIM_STG', 'AIRCRAFT_DIM') }}