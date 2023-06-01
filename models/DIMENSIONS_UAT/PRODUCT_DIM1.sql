{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'PRODUCT_IDENTIFIER',
        alias = 'PRODUCT_DIM'
    )
}}

WITH X as (
SELECT
A.ITEM_NAME,A.PRODUCT_CODE,A.PRODUCT_GROUP,A.PRICE AS PRICE_AMOUNT ,A.CURRENCY AS CURRENCY_CODE,
try_to_date(SUBSTR(A._file,61,10)) as EFFECTIVE_DATETIME
from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }} A
)
select
 {{ dbt_utils.generate_surrogate_key([
     'PRODUCT_CODE','PRODUCT_GROUP'
 ]) }} as PRODUCT_IDENTIFIER,
min(ITEM_NAME) AS ITEM_NAME,
PRODUCT_CODE,
PRODUCT_GROUP,
min(PRICE_AMOUNT) as PRICE_AMOUNT,
min(CURRENCY_CODE) as CURRENCY_CODE,
min(EFFECTIVE_DATETIME) as EFFECTIVE_DATETIME,
CURRENT_TIMESTAMP AS INSERTDATETIME,
NULL as EXPIRY_DATETIME
from X
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}
group by
PRODUCT_CODE,
PRODUCT_GROUP