{{
    config(
        materialized = 'view',
    )
}}

with mny_sum as (
  select  lng_GL_Charges_Id_Nmbr,sum(mny_Distance) OVER (PARTITION by t1.lng_GL_Charges_Id_Nmbr) as total_mny_Distance

  from {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGES') }} t1

left join {{ source('PSS_AMELIARES_DBO', 'TBL_RES_SEGMENTS') }} t4 on t4.LNG_RES_LEGS_ID_NMBR = t1.LNG_RES_LEGS_ID_NMBR and t4._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_DETAIL') }}  t5 on t5.LNG_SKED_DETAIL_ID_NMBR = t4.LNG_SKED_DETAIL_ID_NMBR
  
)

select
case 
    when t9.STR_AGENCY_NAME <> 'MISCHOICE' then 'Amelia'
    else 'Amelia' 
end as "Source",
t1.LNG_RESERVATION_NMBR as "Reservation Nmbr",
t2.STR_REF1 as "PNR",
t5.STR_FLIGHT_NMBR as "Flight Num",
t6.STR_IDENT AS "Departure",
t7.STR_IDENT AS "Arrival",
TO_VARCHAR(TO_DATE(t1.DTM_GL_CHARGES_DATE),'MM/DD/YYYY')  as "Charge Date",
case 
    when t8.STR_TAX_1_NAME='Tax 1' and t8.STR_TAX_CONFIGURATION_NAME='Tax Exempt' then 'Exempt'
    when t8.STR_TAX_1_NAME='Tax 1' and t8.STR_TAX_CONFIGURATION_NAME='MX VAT' then 'Mex VAT'
    when t8.STR_TAX_1_NAME='Tax 1' and t8.STR_TAX_CONFIGURATION_NAME='MX VAT - International' then 'Mex VAT'
    when t8.STR_TAX_1_NAME='Tax 1' and t8.STR_TAX_CONFIGURATION_NAME='General - HST - RC' then 'HST'
    when t8.STR_TAX_1_NAME='Tax 1' and t8.STR_TAX_CONFIGURATION_NAME='Dom Transp Tax (DO)' then 'Dom Transp Tax (DO)'
    else  t8.STR_TAX_1_NAME
  end as 
"Tax Type",
sum(round((t1.MNY_GL_CHARGES_AMOUNT-t1.mny_GL_Charges_Discount)*div0(t5.mny_Distance,t10.total_mny_Distance),2)) as "Net Charge",
--round(div0(sum(round(t1.MNY_GL_CHARGES_TAXES,2)),sum(round(t1.MNY_GL_CHARGES_AMOUNT-t1.mny_GL_Charges_Discount,2)))*100,2) as "Calculated Tax Rate",
sum(round(t1.mny_GL_Charges_Taxes * div0(t5.mny_Distance,t10.total_mny_Distance),2)) as "Taxes",
round(div0("Taxes","Net Charge")*100,2) as "Calculated Tax Rate",
case
    when "Tax Type" = 'GST' then  round(("Calculated Tax Rate"/100)*"Net Charge",2)
    when "Tax Type" = 'QST' then  round(0.05*"Net Charge",2)
    else 0
end as "Taxes GST",
case
    when "Tax Type" = 'HST' then round(("Calculated Tax Rate"/100)*"Net Charge",2)
    else 0
end as "Taxes HST",
case 
    when "Tax Type" = 'QST' then round((("Calculated Tax Rate"-5)/100)*"Net Charge",2)
    else 0
end as "Taxes QST",
case
    when "Tax Type" = 'Mex VAT' then round(("Calculated Tax Rate"/100)*"Net Charge",2)
    else 0
end as "Taxes Mex VAT",
case
    when "Tax Type" = 'Dom Transp Tax (DO)' then round(("Calculated Tax Rate"/100)*"Net Charge",2)
    else 0
end as "Taxes DR"

from {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGES') }} t1
left join {{ source('PSS_AMELIARES_DBO', 'TBL_RES_HEADER') }} t2 on t2.LNG_RESERVATION_NMBR=t1.LNG_RESERVATION_NMBR
left join {{ source('PSS_AMELIARES_DBO', 'TBL_RES_SEGMENTS') }} t4 on t4.LNG_RES_LEGS_ID_NMBR = t1.LNG_RES_LEGS_ID_NMBR and t4._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_DETAIL') }} t5 on t5.LNG_SKED_DETAIL_ID_NMBR = t4.LNG_SKED_DETAIL_ID_NMBR
left join {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} t6 on t6.LNG_AIRPORT_ID_NMBR = t5.LNG_DEP_AIRPORT_ID_NMBR
left join {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} t7 on t7.LNG_AIRPORT_ID_NMBR = t5.LNG_ARR_AIRPORT_ID_NMBR
left join {{ source('PSS_AMELIARES_DBO', 'TBL_TAX_CONFIGURATION') }} t8 on t8.LNG_TAX_CONFIGURATION_ID_NMBR = t1.LNG_TAX_CONFIGURATION_ID_NMBR
left join {{ source('PSS_AMELIARES_DBO', 'TBL_AGENCY') }} t9 on t9.LNG_AGENCY_ID_NMBR=t2.LNG_AGENCY_ID_NMBR
left join mny_sum t10 on t10.lng_GL_Charges_Id_Nmbr=t1.lng_GL_Charges_Id_Nmbr

group by 1,2,3,4,5,6,7,8