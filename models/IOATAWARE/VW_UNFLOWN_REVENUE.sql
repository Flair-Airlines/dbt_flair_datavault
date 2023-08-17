{{
    config(
        materialized = 'view',
    )
}}

select 
TO_VARCHAR(TO_DATE(t1.DTM_GL_CHARGES_DATE),'MM/DD/YYYY') as "Charge Date",
TO_VARCHAR(TO_DATE(t4.DTM_FLIGHT_DATE),'MM/DD/YYYY') as "Flight Date",
TO_VARCHAR(YEAR(t4.DTM_FLIGHT_DATE)) as "Flight Year",
TO_VARCHAR(MONTH(t4.DTM_FLIGHT_DATE)) as "Flight Month",
t1.LNG_RESERVATION_NMBR as "Reservation #",
t2.STR_REF1 as "PNR",
sum(round(t1.MNY_GL_CHARGES_AMOUNT-t1.mny_GL_Charges_Discount,2)) as "Net Charge Total",
sum(round(t1.MNY_GL_CHARGES_TAXES,2)) as "Taxes Total",
sum(round(t1.MNY_GL_CHARGES_AMOUNT-t1.mny_GL_Charges_Discount,2)) + sum(round(t1.MNY_GL_CHARGES_TAXES,2)) as "Charge Total",
case 
			when (t1.lng_GL_Charge_Type_Id_Nmbr = 4 OR (t1.lng_gl_charge_type_id_nmbr = 1028 AND STR_GL_CHARGES_DESC like '%Fare Rebate%')) then 'Base'
			when t1.lng_GL_Charge_Type_Id_Nmbr in (2, 3, 6, 995, 996, 999, 1000, 1001, 1002, 1013,1005) then 'Ancillary'
			when t1.lng_GL_Charge_Type_Id_Nmbr in (1, 5, 1009, 1010, 1012, 1016, 1018, 1019, 1020, 1021, 1022, 1023,1024,1025,1026,1027,1029,1030,1031,1032,1033) then 'FlowThru'
			else 'Unassigned'
end as "Category"

from {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGES') }} t1
left join {{ source('PSS_AMELIARES_DBO', 'TBL_RES_HEADER') }}  t2 on t2.LNG_RESERVATION_NMBR = t1.LNG_RESERVATION_NMBR
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_SEGMENTS') }} t3 on t3.LNG_RES_LEGS_ID_NMBR = t1.LNG_RES_LEGS_ID_NMBR
LEFT join {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_DETAIL') }} t4 on t4.LNG_SKED_DETAIL_ID_NMBR = t3.LNG_SKED_DETAIL_ID_NMBR
group by 1,2,3,4,5,6,10