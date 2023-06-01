{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'AIRLINE_FLIGHT_IDENTIFIER',
        alias = 'AIRLINE_FLIGHT_DIM'
    )
}}
select DISTINCT
 {{ dbt_utils.generate_surrogate_key([
      'AIRLINE_CODE','FLIGHT_NUMBER'
 ]) }} as AIRLINE_FLIGHT_IDENTIFIER,
AIRLINE_CODE, FLIGHT_NUMBER, CURRENT_TIMESTAMP AS INSERTDATETIME
from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}