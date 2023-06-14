{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'CONCAT(LNG_SKED_DETAIL_ID_NMBR,LNG_SKED_DETAIL_ID_NMBR)',
        merge_exclude_columns = ['LNG_SKED_DETAIL_ID_NMBR']
    )
}}

SELECT CONCAT(sked_detail_id_key,LNG_SKED_DETAIL_ID_NMBR) AS LNG_SKED_DETAIL_ID_NMBR, * exclude (sked_detail_id_key,LNG_SKED_DETAIL_ID_NMBR) FROM 
(SELECT SKED_NEW.LNG_SKED_DETAIL_ID_NMBR sked_detail_id_key, SKED_NEW.* FROM FIVETRAN_DATABASE.PSS_AMELIARES_DBO.SKED_DETAIL_DUMMYTABLE SKED_NEW  

union all
 
(SELECT Null as sked_detail_id_key, sked_scd2.* exclude(DBT_SCD_ID,DBT_UPDATED_AT,DBT_VALID_FROM, DBT_VALID_TO) 
FROM DATAVAULT_DEV.DV_AMELIA.SKED_DETAILS_SNAPSHOT sked_scd2 
JOIN FIVETRAN_DATABASE.PSS_AMELIARES_DBO.SKED_DETAIL_DUMMYTABLE  SKED_NEW ON
sked_scd2.LNG_SKED_DETAIL_ID_NMBR = SKED_NEW.LNG_SKED_DETAIL_ID_NMBR and sked_scd2.DBT_VALID_TO  is null))