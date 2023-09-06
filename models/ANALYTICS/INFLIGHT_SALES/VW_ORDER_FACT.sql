{{
    config(
        materialized = 'view',
    )
}}

SELECT 
    ORDER_IDENTIFIER as "Order Identifier", 
    ORDER_ID as "Order Id",
    ORDER_STANDARD_DATETIME as "Order Standard Datetime",
    ORDER_LOCAL_DATATIME as "Order Local Datetime",
    ORDER_TOTAL_AMOUNT as "Order Total Amount",
    INSERTDATETIME as "Insert Datetime"
FROM {{ source('INFLIGHTSALES_FACT_STG', 'ORDER_FACT') }}