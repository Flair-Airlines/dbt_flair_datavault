{{
    config(
        materialized = 'view',
    )
}}

SELECT 
    DISCOUNT_REASON_IDENTIFIER as "Discount Reason Identifier", 
    DISCOUNT_REASON_NAME as "Discount Reason Name",
    INSERTDATETIME as "Insert Datetime"
FROM {{ source('INFLIGHTSALES_DIM_STG', 'DISCOUNT_REASON_DIM') }}