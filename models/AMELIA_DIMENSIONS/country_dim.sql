{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'COUNTRY_SURROGATE_ID_HASH'
    )
}}

SELECT 
{{ dbt_utils.generate_surrogate_key([
     'STR_COUNTRY_IDENT'	
 ]) }} as COUNTRY_SURROGATE_ID_HASH,
LNG_COUNTRY_ID_NMBR,
DTM_CREATION_DATE,		
STR_COUNTRY_DESC,		
STR_COUNTRY_IDENT,		
LNG_LAST_MOD_USER_ID_NMBR,
DTM_CREATION_DATE as TSP_TIMESTAMP,
STR_DELETED_FLAG,
DTM_LAST_MOD_DATE,
LNG_CREATION_USER_ID_NMBR,
TRUE as LATEST_INDICATOR,
CURRENT_TIMESTAMP() AS INSERTED_TS,
to_timestamp('1900-01-01','YYYY-MM-DD') as EFFECTIVE_DATETIME,
NULL AS EXPIRY_DATETIME,
{{ dbt_utils.generate_surrogate_key([
     'STR_COUNTRY_IDENT'	
 ]) }} as ORIGINAL_SURROGATE_ID_HASH	
FROM {{ source('PSS_AMELIARES_DBO', 'TBL_COUNTRY') }}	