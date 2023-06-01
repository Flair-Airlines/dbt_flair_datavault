{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'CURRENCY_CODE'
    )
}}

select DISTINCT 
trim(CURRENCY_CODE) as CURRENCY_CODE,
CURRENCY_NAME,
CURRENT_TIMESTAMP AS INSERTDATETIME ,
CURRENCY_SYMBOL
 from {{ ref('CURRENCY_PRE_DIM') }}
 {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where INSERTDATETIME > IFNULL((select max(INSERTDATETIME) from {{ this }}),'1900-01-01')

{% endif %}