{{
    config(
        materialized = 'incremental',
        unique_key = 'AIRCRAFT_IDENTIFIER',
        incremental_strategy = 'merge',
        merge_update_columns = ['AIRCRAFT_IDENTIFIER'],
        
    )
}}
select DISTINCT
 {{ dbt_utils.generate_surrogate_key([
      'TAILNUMBER'
 ]) }} as AIRCRAFT_IDENTIFIER,
TAILNUMBER, CURRENT_TIMESTAMP AS INSERTDATETIME
from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }}




