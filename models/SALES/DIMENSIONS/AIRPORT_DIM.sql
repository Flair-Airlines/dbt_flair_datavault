{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'AIRPORT_CODE'
    )
}}
select DISTINCT
    DEPARTURE AS AIRPORT_CODE,
	NULL AS AIRPORT_NAME,
	CURRENT_TIMESTAMP AS INSERTDATETIME
from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }}
WHERE DEPARTURE IS NOT NULL 
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
 AND INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}
UNION 
select DISTINCT
    ARRIVAL AS AIRPORT_CODE,
	NULL AS AIRPORT_NAME,
	CURRENT_TIMESTAMP AS INSERTDATETIME
from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }}

WHERE ARRIVAL IS NOT NULL
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
 AND INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}