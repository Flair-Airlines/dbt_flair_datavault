{{ 
config(
    materialized='incremental',
    unique_key=['_FILE','_LINE'],
    incremental_strategy='merge',
    merge_update_columns = ['ITEM_NAME','PRODUCT_GROUP'],
    post_hook = "{{update_flights()}}"
)
}}

SELECT 
"_FILE",
"_LINE",
"_MODIFIED",
AIRLINE_CODE,
ORDER_CREATED_UTC_,
ORDER_CREATED_LOCAL_,
ORDER_ID,
FLIGHT_NUMBER,
DEPARTURE,
ARRIVAL,
TAILNUMBER,
DEPARTURE_DATE_UTC_,
DEPARTURE_DATE_LOCAL_,
PRODUCT_CODE,
ITEM_NAME,
PRODUCT_GROUP,
QUANTITY,
PRICE,
BASE,
TAX_INFO,
TAX,
DISCOUNT,
DISCOUNT_REASON,
TOTAL_GROSS,
CURRENCY,
ORDER_TOTAL,
_FIVETRAN_SYNCED,
NULL AS  CLEAN_RULE_LIST,
NULL AS CLEAN_TIMESTAMP
from {{ source('INFLIGHTSALES_LIFEBOB1', 'FLIGHTS') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where _FIVETRAN_SYNCED > IFNULL((select max(_FIVETRAN_SYNCED) from {{ this }}),'1900-01-01')

{% endif %}


