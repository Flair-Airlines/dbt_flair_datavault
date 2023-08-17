{{
    config(
        materialized = 'view',
    )
}}
with mny_sum as (
  select  lng_GL_Charges_Id_Nmbr,sum(mny_Distance) OVER (PARTITION by t2.lng_GL_Charges_Id_Nmbr) as total_mny_Distance

  from {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGES') }} t2

left join {{ source('PSS_AMELIARES_DBO', 'TBL_RES_SEGMENTS') }} t4 on t4.LNG_RES_LEGS_ID_NMBR = t2.LNG_RES_LEGS_ID_NMBR and t4._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_DETAIL') }} t5 on t5.LNG_SKED_DETAIL_ID_NMBR = t4.LNG_SKED_DETAIL_ID_NMBR
)
select 
TO_VARCHAR(TO_DATE(t5.DTM_FLIGHT_DATE),'YYYY/MM/DD') as "Flight Date",
TO_VARCHAR(TO_TIME(t5.DTM_FLIGHT_DATE),'HH24:MI:SS') as "Flight time",
t5.LNG_SKED_DETAIL_ID_NMBR as "Flight ID",
t6.STR_IDENT as "Departure",
t7.STR_IDENT as "Arrival",
t1.LNG_RESERVATION_NMBR as "Reservation Number",
t8.LNG_LEG_NMBR as "Leg Number", 
t1.STR_REF1 as "PNR",
t4.STR_RES_CHECKIN_BOARDED as "Check In Boarded",
t4.STR_RES_STATUS as "Status",
t10.STR_FIRST_NAME as "First Name",
t10.STR_LAST_NAME as "Last Name",
t3.STR_GL_CHARGE_TYPE_DESC AS "Charge Type",
round((t2.MNY_GL_CHARGES_AMOUNT-t2.mny_GL_Charges_Discount)*div0(t5.mny_Distance,t11.total_mny_Distance),2) as "Net Charge",
t6.STR_IDENT as "Leg Start",
t7.STR_IDENT as "Leg End",
TO_VARCHAR(TO_DATE(t2.DTM_GL_CHARGES_DATE),'MM/DD/YYYY') as "Charge Date"

from {{ source('PSS_AMELIARES_DBO', 'TBL_RES_HEADER') }} t1
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGES') }} t2 on t2.LNG_RESERVATION_NMBR = t1.LNG_RESERVATION_NMBR and t2.LNG_GL_CHARGE_TYPE_ID_NMBR=1
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGE_TYPE_DEFINITION') }} t3 on t3.LNG_GL_CHARGE_TYPE_ID_NMBR = t2.LNG_GL_CHARGE_TYPE_ID_NMBR
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_LEGS') }} t8 on t8.LNG_RES_LEGS_ID_NMBR = t2.LNG_RES_LEGS_ID_NMBR
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_SEGMENTS') }} t4 on t4.LNG_RES_LEGS_ID_NMBR = t2.LNG_RES_LEGS_ID_NMBR
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_DETAIL') }} t5 on t5.LNG_SKED_DETAIL_ID_NMBR = t4.LNG_SKED_DETAIL_ID_NMBR
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} t6 on t6.LNG_AIRPORT_ID_NMBR = t5.LNG_DEP_AIRPORT_ID_NMBR
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} t7 on t7.LNG_AIRPORT_ID_NMBR = t5.LNG_ARR_AIRPORT_ID_NMBR
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_PAX_GROUP') }} t9 on t9.LNG_RES_PAX_GROUP_ID_NMBR=t8.LNG_RES_PAX_GROUP_ID_NMBR
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_PAX') }} t10 on t10.LNG_PAX_ID_NMBR = t9.LNG_PAX_ID_NMBR 
left join mny_sum t11 on t11.lng_GL_Charges_Id_Nmbr=t2.lng_GL_Charges_Id_Nmbr

where "Check In Boarded" = 'B'
