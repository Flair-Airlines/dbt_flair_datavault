{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'PASSENGER_SURROGATE_ID_HASH'
    )
}}
WITH province as(
    select PROVINCE_SURROGATE_ID_HASH,LNG_PROVINCE_ID_NMBR from {{ ref('province_dim') }}
),

country as(
    select COUNTRY_SURROGATE_ID_HASH,LNG_COUNTRY_ID_NMBR from {{ ref('country_dim') }}
)
select
{{ dbt_utils.generate_surrogate_key([
     'PA.LNG_PAX_ID_NMBR'
]) }} as PASSENGER_SURROGATE_ID_HASH,
LNG_PAX_ID_NMBR,
STR_PRE_BOARD,
DTM_DOB_DATE,
STR_GENDER,
STR_NATIONALITY,
PA.DTM_LAST_MOD_DATE,
PA.DTM_CREATION_DATE as TSP_TIMESTAMP,
PA.LNG_CREATION_USER_ID_NMBR,
PA.DTM_CREATION_DATE,
STR_LAST_NAME,
STR_ACTIVE_FLAG,
STR_CITY,
PA.LNG_COUNTRY_ID_NMBR,
STR_POSTAL_CODE,
STR_NOTES,
STR_NAME_KEY,
STR_FIRST_NAME,
STR_MIDDLE_NAME,
LNG_CATEGORY_ID_NMBR,
PA.LNG_LAST_MOD_USER_ID_NMBR,
PA.LNG_PROVINCE_ID_NMBR,
LNG_PREF_LANGUAGE_ID_NMBR,
TRUE as LATEST_INDICATOR,
CURRENT_TIMESTAMP() AS INSERTED_TS,
to_timestamp('1900-01-01','YYYY-MM-DD') as EFFECTIVE_DATETIME,
NULL AS EXPIRY_DATETIME,
{{ dbt_utils.generate_surrogate_key([
     'PA.LNG_PAX_ID_NMBR'
]) }} as ORIGINAL_SURROGATE_ID_HASH,
PD.PROVINCE_SURROGATE_ID_HASH,			
CD.COUNTRY_SURROGATE_ID_HASH	
from {{ source('PSS_AMELIARES_DBO', 'TBL_PAX') }} PA
LEFT JOIN province PD ON PD.LNG_PROVINCE_ID_NMBR = PA.LNG_PROVINCE_ID_NMBR
LEFT JOIN country CD ON CD.LNG_COUNTRY_ID_NMBR = PA.LNG_COUNTRY_ID_NMBR
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
 where INSERTED_TS > IFNULL((select max(INSERTED_TS) from {{ this }}),'1900-01-01')

{% endif %}
