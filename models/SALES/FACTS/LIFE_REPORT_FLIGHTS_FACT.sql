{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'LIFE_REPORT_FLIGHTS_FACT_IDENTIFIER'
    )
}}
select 

{{ dbt_utils.generate_surrogate_key([
      '_LINE','_FILE'
 ]) }} as LIFE_REPORT_FLIGHTS_FACT_IDENTIFIER,
 {{ dbt_utils.generate_surrogate_key([
      'TAILNUMBER'
 ]) }} as AIRCRAFT_IDENTIFIER,
 {{ dbt_utils.generate_surrogate_key([
      'AIRLINE_CODE','FLIGHT_NUMBER','DEPARTURE_DATE_UTC_'
 ]) }} as FLIGHT_IDENTIFIER,
 CURRENCY AS CURRENCY_CODE,
  {{ dbt_utils.generate_surrogate_key([
      'PRODUCT_CODE','PRODUCT_GROUP'
 ]) }} as PRODUCT_IDENTIFIER,
 {{ dbt_utils.generate_surrogate_key([
      'ORDER_ID'
 ]) }} as ORDER_IDENTIFIER,
  {{ dbt_utils.generate_surrogate_key([
      'TAX_INFO'
 ]) }} as TAX_INFO_IDENTIFIER ,
  {{ dbt_utils.generate_surrogate_key([
      'DISCOUNT_REASON'
 ]) }} as DISCOUNT_REASON_IDENTIFIER,
_LINE,
DEPARTURE as DEPARTURE_AIRPORT_CODE ,
ARRIVAL as ARRIVAL_AIRPORT_CODE,
QUANTITY, 
PRICE AS PRICE_AMOUNT,
BASE AS BASE_AMOUNT,
DISCOUNT AS DISCOUNT_AMOUNT,
TAX AS TAX_AMOUNT,
TOTAL_GROSS AS TOTAL_GROSS_AMOUNT,
CURRENT_TIMESTAMP AS INSERTDATETIME,
_FILE
 from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}