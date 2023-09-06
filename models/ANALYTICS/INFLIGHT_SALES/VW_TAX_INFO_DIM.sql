{{
    config(
        materialized = 'view',
    )
}}

SELECT 
    TAX_INFO_IDENTIFIER as "Tax Info Identifier", 
    TAX_INFO_NAME as "Tax Info Name", 
    INSERTDATETIME as "Insert Datetime"
FROM {{ source('INFLIGHTSALES_DIM_STG', 'TAX_INFO_DIM') }}
