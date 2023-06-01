{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'DISCOUNT_REASON_IDENTIFIER'
    )
}}

select DISTINCT
 {{ dbt_utils.generate_surrogate_key([
      'DISCOUNT_REASON'
 ]) }} as DISCOUNT_REASON_IDENTIFIER,
DISCOUNT_REASON as DISCOUNT_REASON_NAME , CURRENT_TIMESTAMP AS INSERTDATETIME
from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}
