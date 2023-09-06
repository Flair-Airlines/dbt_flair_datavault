{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ORDER_IDENTIFIER',
        alias = 'ORDER_FACT'
    )
}}
select DISTINCT
 {{ dbt_utils.generate_surrogate_key([
      'ORDER_ID'
 ]) }} as ORDER_IDENTIFIER,
ORDER_ID ,ORDER_CREATED_LOCAL_ as ORDER_LOCAL_DATATIME,
ORDER_CREATED_UTC_ as ORDER_STANDARD_DATETIME,
ORDER_TOTAL as ORDER_TOTAL_AMOUNT , CURRENT_TIMESTAMP AS INSERTDATETIME
from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}