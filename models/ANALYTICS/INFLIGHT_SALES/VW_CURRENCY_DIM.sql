{{
    config(
        materialized = 'view',
    )
}}

SELECT 
    CURRENCY_CODE as "Currency Code", 
    CURRENCY_NAME as "Currency Name",
    INSERTDATETIME as "Insert Datetime",
    CURRENCY_SYMBOL as "Currency Symbol"
FROM {{ source('INFLIGHTSALES_DIM_STG', 'CURRENCY_DIM') }}