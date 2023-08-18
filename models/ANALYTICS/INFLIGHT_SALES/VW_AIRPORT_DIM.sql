{{
    config(
        materialized = 'view',
    )
}}

SELECT 
    AIRPORT_CODE as "Airport Code", 
    AIRPORT_NAME as "Airport Name", 
    INSERTDATETIME as "Insert Datetime"
FROM {{ source('INFLIGHTSALES_DIM_STG', 'AIRPORT_DIM') }}
