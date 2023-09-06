{{
    config(
        materialized = 'view',
        incremental_strategy = 'merge',
        unique_key = 'LNG_GL_PAYMENT_METHOD_ID_NMBR'
    )
}}
select 
{{ dbt_utils.generate_surrogate_key([
      'LNG_GL_PAYMENT_METHOD_ID_NMBR'
 ]) }} as GL_PAYMENT_METHOD_SURROGATE_ID_HASH,
LNG_GL_PAYMENT_METHOD_ID_NMBR,
LNG_LAST_MOD_USER_ID_NMBR,
LNG_CREATION_USER_ID_NMBR,
LNG_CREDITCARD_VERIFY_ID_NMBR,
DTM_CREATION_DATE,
STR_TICKETNMBR_REQ,
LNG_GL_PAYMENT_PROCESS_FEE_TYPE_ID_NMBR,
STR_ACTIVE_FLAG,
STR_SALE_ACC_CODE,
STR_USAGE_ACC_CODE,
MNY_PROCESS_AMT_MIN,
STR_GL_PAYMENT_METHOD_IDENT,
STR_GL_PAYMENT_METHOD_FLAG,
DTM_LAST_MOD_DATE,
STR_GL_PAYMENT_METHOD_DESC,
STR_PMT_PROCESS_FLAG,
STR_GL_PAYMENT_METHOD_RECEIPT,
MNY_PROCESS_AMT,
DTM_CREATION_DATE as TSP_TIMESTAMP,
MNY_PROCESS_AMT_MAX,
TRUE AS LATEST_INDICATOR,
CURRENT_TIMESTAMP() AS INSERTED_TS,
to_timestamp('1900-01-01','YYYY-MM-DD') as EFFECTIVE_DATETIME,
NULL AS EXPIRY_DATETIME,
{{ dbt_utils.generate_surrogate_key([
      'LNG_GL_PAYMENT_METHOD_ID_NMBR'
 ]) }} as ORIGINAL_SURROGATE_ID_HASH

FROM {{ source('PSS_AMELIARES_DBO' ,'TBL_GL_PAYMENT_METHOD') }}
