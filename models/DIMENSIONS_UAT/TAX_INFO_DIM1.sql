{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'TAX_INFO_IDENTIFIER',
        alias = 'TAX_INFO_DIM'
    )
}}
select DISTINCT
 {{ dbt_utils.generate_surrogate_key([
      'TAX_INFO'
 ]) }} as TAX_INFO_IDENTIFIER ,
TAX_INFO as TAX_INFO_NAME, CURRENT_TIMESTAMP AS INSERTDATETIME
from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}
