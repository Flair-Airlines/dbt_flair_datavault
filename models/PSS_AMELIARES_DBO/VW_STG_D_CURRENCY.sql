{{
    config(
        materialized = 'view',
        incremental_strategy = 'merge',
        unique_key = 'CURRENCY_SURROGATE_ID_HASH'
    )
}}
select 
{{ dbt_utils.generate_surrogate_key([
      'STR_CURRENCY_IDENT'
 ]) }} as CURRENCY_SURROGATE_ID_HASH,
LNG_CURRENCY_ID_NMBR,
STR_CURRENCY_DESC,
STR_CURRENCY_IDENT,
STR_DELETED_FLAG,
DTM_LAST_MOD_DATE,
STR_CURRENCY_FORMAT,
LNG_LAST_MOD_USER_ID_NMBR,
DTM_CREATION_DATE,
LNG_CREATION_USER_ID_NMBR,
TRUE AS LATEST_INDICATOR,
CURRENT_TIMESTAMP() AS INSERTED_TS,
to_timestamp('1900-01-01','YYYY-MM-DD') as EFFECTIVE_DATETIME,
NULL AS EXPIRY_DATETIME,
{{ dbt_utils.generate_surrogate_key([
      'STR_CURRENCY_IDENT'
 ]) }} as ORIGINAL_SURROGATE_ID_HASH

FROM {{ source('PSS_AMELIARES_DBO' ,'TBL_CURRENCY') }}

