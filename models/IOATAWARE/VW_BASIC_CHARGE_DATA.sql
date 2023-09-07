{{
    config(
        materialized = 'view'
    )
}}

select
case 
    when t6.STR_AGENCY_NAME <> 'MISCHOICE' then 'Amelia'
    else 'Amelia' 
end as "SOURCE",
t1.LNG_RESERVATION_NMBR as RES_NMBR,
--add this column for testing
TO_VARCHAR(convert_timezone('UTC', 'America/Denver', t1.dtm_GL_Charges_Date),'MM/DD/YYYY') as "Charge Date",
to_varchar(convert_timezone('UTC', 'America/Denver',t1.DTM_GL_CHARGES_DATE),'MM/DD/YYYY HH12:MI:SS AM') as CHARGE_DATE,
--t1.DTM_GL_CHARGES_DATE as CHARGE_DATE,
t2.STR_GL_CHARGE_TYPE_DESC as CHARGE_TYPE,
round(t1.MNY_GL_CHARGES_AMOUNT-t1.MNY_GL_CHARGES_DISCOUNT,2) as CHARGE_AMT,
round(t1.MNY_GL_CHARGES_TAXES,2) as "CHARGE TAXES",
round(t1.MNY_GL_CHARGES_AMOUNT-t1.MNY_GL_CHARGES_DISCOUNT,2) + round(t1.MNY_GL_CHARGES_TAXES,2) as "CHARGE TOTAL",
t3.STR_REF1 as PNR,
t6.STR_AGENCY_NAME as SALES_AGENCY

from {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGES') }} t1
left join {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGE_TYPE_DEFINITION') }} t2 on t2.LNG_GL_CHARGE_TYPE_ID_NMBR = t1.LNG_GL_CHARGE_TYPE_ID_NMBR  and t2._fivetran_deleted = FALSE
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_HEADER') }} t3 on t3.LNG_RESERVATION_NMBR = t1.LNG_RESERVATION_NMBR and t3._fivetran_deleted = FALSE
LEFT join {{ source('PSS_AMELIARES_DBO', 'TBL_AGENCY') }} t6 on t6.LNG_AGENCY_ID_NMBR = t3.LNG_AGENCY_ID_NMBR and t6._fivetran_deleted = FALSE