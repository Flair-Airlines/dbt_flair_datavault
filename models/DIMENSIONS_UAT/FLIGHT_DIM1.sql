{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'FLIGHT_IDENTIFIER',
        alias = 'FLIGHT_DIM'
    )
}}
select DISTINCT
 {{ dbt_utils.generate_surrogate_key([
      'AIRLINE_CODE','FLIGHT_NUMBER','DEPARTURE_DATE_UTC_'
 ]) }} as FLIGHT_IDENTIFIER,
AIRLINE_CODE, FLIGHT_NUMBER, 
case when try_to_timestamp(DEPARTURE_DATE_UTC_ ,'DD.MM.YYYY HH24:MI:SS') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(DEPARTURE_DATE_UTC_ ,'DD.MM.YYYY HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')

when try_to_timestamp(DEPARTURE_DATE_UTC_ ,'MM/DD/YYYY HH12:MI:SS AM') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(DEPARTURE_DATE_UTC_,'MM/DD/YYYY HH12:MI:SS AM'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')


when try_to_timestamp(DEPARTURE_DATE_UTC_,'MM/DD/YYYY, HH12:MI:SS AM') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(DEPARTURE_DATE_UTC_,'MM/DD/YYYY, HH12:MI:SS AM'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')
else NULL
end as DEPARTURE_STANDARD_DATETIME,

case when try_to_timestamp(DEPARTURE_DATE_LOCAL_,'DD.MM.YYYY HH24:MI:SS') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(DEPARTURE_DATE_LOCAL_,'DD.MM.YYYY HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')

when try_to_timestamp(DEPARTURE_DATE_LOCAL_,'MM/DD/YYYY HH12:MI:SS AM') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(DEPARTURE_DATE_LOCAL_,'MM/DD/YYYY HH12:MI:SS AM'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')


when try_to_timestamp(DEPARTURE_DATE_LOCAL_,'MM/DD/YYYY, HH12:MI:SS AM') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(DEPARTURE_DATE_LOCAL_,'MM/DD/YYYY, HH12:MI:SS AM'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')

else NULL
    end as DEPARTURE_LOCAL_DATETIME,

CURRENT_TIMESTAMP AS INSERTDATETIME
from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}
