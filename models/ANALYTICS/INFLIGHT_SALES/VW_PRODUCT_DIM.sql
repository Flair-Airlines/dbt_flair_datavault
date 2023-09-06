{{
    config(
        materialized = 'view',
    )
}}

SELECT 
    PRODUCT_IDENTIFIER as "Product Identifier", 
    ITEM_NAME as "Item Name", 
    PRODUCT_CODE as "Product Code", 
    PRODUCT_GROUP as "Product Group", 
    PRICE_AMOUNT as "Price Amount", 
    CURRENCY_CODE as "Currency Code", 
    EFFECTIVE_DATETIME as "Effective Datetime", 
    INSERTDATETIME as "Insert Datetime", 
    EXPIRY_DATETIME as "Expiry Datetime"
FROM {{ source('INFLIGHTSALES_DIM_STG', 'PRODUCT_DIM') }}