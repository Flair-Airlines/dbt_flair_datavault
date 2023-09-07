{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ORDER_IDENTIFIER'
    )
}}
select DISTINCT
 {{ dbt_utils.generate_surrogate_key([
      'ORDER_ID'
 ]) }} as ORDER_IDENTIFIER,
ORDER_ID ,
case when try_to_timestamp(ORDER_CREATED_UTC_ ,'DD.MM.YYYY HH24:MI:SS') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(ORDER_CREATED_UTC_ ,'DD.MM.YYYY HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')

when try_to_timestamp(ORDER_CREATED_UTC_ ,'MM/DD/YYYY HH12:MI:SS AM') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(ORDER_CREATED_UTC_,'MM/DD/YYYY HH12:MI:SS AM'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')


when try_to_timestamp(ORDER_CREATED_UTC_,'MM/DD/YYYY, HH12:MI:SS AM') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(ORDER_CREATED_UTC_,'MM/DD/YYYY, HH12:MI:SS AM'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')
else NULL
end as ORDER_STANDARD_DATETIME,

case when try_to_timestamp(ORDER_CREATED_LOCAL_,'DD.MM.YYYY HH24:MI:SS') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(ORDER_CREATED_LOCAL_,'DD.MM.YYYY HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')

when try_to_timestamp(ORDER_CREATED_LOCAL_,'MM/DD/YYYY HH12:MI:SS AM') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(ORDER_CREATED_LOCAL_,'MM/DD/YYYY HH12:MI:SS AM'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')


when try_to_timestamp(ORDER_CREATED_LOCAL_,'MM/DD/YYYY, HH12:MI:SS AM') is not null
then  try_to_timestamp(TO_VARCHAR(try_to_timestamp(ORDER_CREATED_LOCAL_,'MM/DD/YYYY, HH12:MI:SS AM'),'YYYY/MM/DD HH24:MI:SS'),'YYYY/MM/DD HH24:MI:SS')

else NULL
    end as ORDER_LOCAL_DATATIME,
ORDER_TOTAL as ORDER_TOTAL_AMOUNT , CURRENT_TIMESTAMP AS INSERTDATETIME
from {{ source('INFLIGHTSALES_LIFEBOB', 'FLIGHTS') }}
--where ORDER_CREATED_LOCAL_  like '%.%'
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}