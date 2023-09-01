{{
    config(
        materialized = 'view'
    )
}}


    
with mny_sum as (
  select  lng_GL_Charges_Id_Nmbr,sum(mny_Distance) OVER (PARTITION by t1.lng_GL_Charges_Id_Nmbr) as total_mny_Distance

  from FIVETRAN_DATABASE.PSS_AMELIARES_DBO.TBL_GL_CHARGES t1

left join FIVETRAN_DATABASE.PSS_AMELIARES_DBO.TBL_RES_SEGMENTS t4 on t4.LNG_RES_LEGS_ID_NMBR = t1.LNG_RES_LEGS_ID_NMBR and t4._FIVETRAN_DELETED = 'FALSE'
left join FIVETRAN_DATABASE.PSS_AMELIARES_DBO.TBL_SKED_DETAIL t5 on t5.LNG_SKED_DETAIL_ID_NMBR = t4.LNG_SKED_DETAIL_ID_NMBR
  
)
select
 TO_VARCHAR(convert_timezone('UTC', 'America/Denver', t1.dtm_GL_Charges_Date),'MM/DD/YYYY') as "Charge Date",
t1.LNG_RESERVATION_NMBR as "Reservation Nmbr",
t5.LNG_SKED_DETAIL_ID_NMBR as "Sked Detail Id Nmbr",
TO_VARCHAR(TO_DATE(t5.DTM_LOCAL_ETD_DATE),'MM/DD/YYYY') AS "Flight Date",

'' AS "Charge Delta",
t6.STR_IDENT as "Departure",
t7.STR_IDENT as "Arrival",
t1.LNG_RES_LEGS_ID_NMBR as "Res Legs Id Nmbr",
t1.LNG_GL_CHARGE_TYPE_ID_NMBR as "Charge Type",
round((t1.MNY_GL_CHARGES_AMOUNT-t1.mny_GL_Charges_Discount)*div0(t5.mny_Distance,t17.total_mny_Distance),2) as "Net Charge",
round(t1.mny_GL_Charges_Taxes * div0(t5.mny_Distance,t17.total_mny_Distance),2) as "Taxes",
round(mny_GL_Charges_Total * div0(t5.mny_Distance,t17.total_mny_Distance),2) as "Total Charge" ,
t2.LNG_LEG_NMBR as "lng Leg Nmbr",
t8.STR_GL_CHARGE_TYPE_DESC  as "GL Charge Type Desc",
t5.STR_FLIGHT_NMBR as "Flight_Nmbr",
t1.STR_GL_CHARGES_DESC as "GL Charges Desc",
t3.STR_REF1 as "PNR",
t9.STR_CURRENCY_IDENT as "Currency Ident",
t4.LNG_RES_SEGMENTS_ID_NMBR as "Res Segments Id Nmbr",
case 
    WHEN t12.str_country_desc = 'Canada' AND t13.str_country_desc = 'Canada' THEN 'Domestic'
    ELSE 'International'
end as "Transborder",
t2.STR_LEG_STATUS as "str Leg Status",
t5.STR_AIRLINE_CODES_IDENT as "Airline Codes Ident",
round(t5.MNY_DISTANCE) as "Distance",
round(t5.MNY_DISTANCE) as "Leg Total Distance",
t14.STR_AGENCY_NAME as "Agency_Name",
round(div0(t5.mny_Distance,t17.total_mny_Distance)*100,1) AS "Percent",
case 
	when (t1.lng_GL_Charge_Type_Id_Nmbr = 4 OR (t1.lng_gl_charge_type_id_nmbr = 1028 AND STR_GL_CHARGES_DESC like '%Fare Rebate%')) then 'Base'
	when t1.lng_GL_Charge_Type_Id_Nmbr in (2, 3, 6, 995, 996, 999, 1000, 1001, 1002, 1013,1005) then 'Ancillary'
	when t1.lng_GL_Charge_Type_Id_Nmbr in (1, 5, 1009, 1010, 1012, 1016, 1018, 1019, 1020, 1021, 1022, 1023,1024,1025,1026,1027,1029,1030,1031,1032,1033) then 'FlowThru'
	else 'Unassigned'
end as "Category",
t1.LNG_GL_CHARGE_TYPE_ID_NMBR as "GL Charge Type Id Nmbr",
--str_Promo_Code
case 
    when t15.STR_TAX_1_NAME='Tax 1' and t15.STR_TAX_CONFIGURATION_NAME='Tax Exempt' then 'Exempt'
    when t15.STR_TAX_1_NAME='Tax 1' and t15.STR_TAX_CONFIGURATION_NAME='MX VAT' then 'Mex VAT'
    when t15.STR_TAX_1_NAME='Tax 1' and t15.STR_TAX_CONFIGURATION_NAME='MX VAT - International' then 'Mex VAT'
    when t15.STR_TAX_1_NAME='Tax 1' and t15.STR_TAX_CONFIGURATION_NAME='General - HST - RC' then 'HST'
    when t15.STR_TAX_1_NAME='Tax 1' and t15.STR_TAX_CONFIGURATION_NAME='Dom Transp Tax (DO)' then 'Dom Transp Tax (DO)'
    else  t15.STR_TAX_1_NAME
  end as "Tax Name",
round(div0("Taxes","Net Charge"),2)*100||'%' AS "Tax Percentage",
round(t3.MNY_EXCHANGE_RATE*100,2) as "Exchange Rate",
t16.str_User_Name as "Last Mod Sales User",
t6.STR_IDENT as "Leg Departure",
t7.STR_IDENT as "Leg Arrival",
split_part(t1.STR_GL_CHARGES_DESC,'-',-1) as "Description",
case 
    when t14.lng_Agency_Id_Nmbr in ('1','8','275') then 'Direct'
	else 'InDirect' 
end as "Channel",
 1 as "Local",
case 
			when t1.lng_GL_Charge_Type_Id_Nmbr = 1 then 'Airport Improvement Fee' -- OK
			when t1.lng_GL_Charge_Type_Id_Nmbr = 3 then 'Cancellation' -- OK
			when t1.lng_GL_Charge_Type_Id_Nmbr = 4 OR (t1.lng_gl_charge_type_id_nmbr = 1028 AND STR_GL_CHARGES_DESC like '%Fare Rebate%') then 'Fare' -- OK
			when t1.lng_GL_Charge_Type_Id_Nmbr = 5 then 'Air Traveller Security Charge' -- OK

			when t1.lng_GL_Charge_Type_Id_Nmbr = 995 OR (UPPER(t1.str_GL_Charges_Desc)) like '%NO SHOW%' then 'No Show' -- OK
			when t1.lng_GL_Charge_Type_Id_Nmbr IN (999,1005) AND (UPPER(t1.str_GL_Charges_Desc) like '%NON REFUNDABLE%' OR UPPER(t1.str_GL_Charges_Desc) like '%NON-REFUNDABLE%') then 'Non Refundable' -- OK
			when t1.lng_GL_Charge_Type_Id_Nmbr = 1000 then 'Call Center Fee' -- OK
			when t1.lng_GL_Charge_Type_Id_Nmbr = 1001 then 'Seat Assignment' -- OK
			
			when t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002)
			and (UPPER(t1.str_GL_Charges_Desc) like '%ACF%'
            OR  t1.str_GL_Charges_Desc like '%Airport Check-in%'
			) then 'ACF - Prepaid'
			
			when (UPPER(t1.str_GL_Charges_Desc) like '%GATE%'
			) then 'Carry On - Gate' 
			
			when (-- OK
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002,1005)
			and UPPER(t1.str_GL_Charges_Desc) like '%CARRY ON%'
			) then 'Carry On' 
			
			when (-- OK
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002)
			and (UPPER(t1.str_GL_Charges_Desc) like '%CHK BAG%' 
--				or UPPER(t1.str_GL_Charges_Desc) like '%CHECKED BAG%'
				or UPPER(t1.str_GL_Charges_Desc) like '%CHECKED%'
                or UPPER(t1.str_GL_Charges_Desc) like '%CHECKE%'
                or UPPER(t1.str_GL_Charges_Desc) like '%CHK%'
				or UPPER(t1.str_GL_Charges_Desc) like '%IN 1ST%'
				or UPPER(t1.str_GL_Charges_Desc) like '%IN 2ND%'
				or UPPER(t1.str_GL_Charges_Desc) like '%IN 3RD%'
                or UPPER(t1.str_GL_Charges_Desc) like '%IN 4TH%'
                or UPPER(t1.str_GL_Charges_Desc) like '%IN 5TH%'
				)
			) then 'Checked Bag' 
			
			when (-- OK
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002)
			and (UPPER(t1.str_GL_Charges_Desc) like '%SPORT%' 
				or UPPER(t1.str_GL_Charges_Desc) like '%GOLF%'
				or UPPER(t1.str_GL_Charges_Desc) like '%HOCKEY%'
				or UPPER(t1.str_GL_Charges_Desc) like '%BICYCLE%'
				or UPPER(t1.str_GL_Charges_Desc) like '%SNOWBOARD%'
				)
			) then 'Sports' 
			
			when (
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002)
			and (UPPER(t1.str_GL_Charges_Desc) like '%PRIORITY%')
			) then 'Priority Boarding' 
			
			when (
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002)
			and (UPPER(t1.str_GL_Charges_Desc) like '%PET IN%')
			) then 'Pet in Cabin' 
			
			when (-- OK
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002) 
			and (UPPER(t1.str_GL_Charges_Desc) like '%OVERWEIGHT%' 
				or UPPER(t1.str_GL_Charges_Desc) like '%OVERSIZE%'
				or UPPER(t1.str_GL_Charges_Desc) like '%EXCESS%')
			) then 'Misc Bag Charges' 
			
			
			
			when (-- OK
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002)
			and (
			UPPER(t1.str_GL_Charges_Desc) like '%BASIC BUNDLE%'
			or UPPER(t1.str_GL_Charges_Desc) like '%BASIC B%'
			)
			) then 'Basic Bundle' 
			
			when (-- OK
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002)
			and (UPPER(t1.str_GL_Charges_Desc) like '%BIG BUNDLE%'
			or UPPER(t1.str_GL_Charges_Desc) like '%BIG B%'
			)
			) then 'Big Bundle' 
			
			when (
			t1.lng_GL_Charge_Type_Id_Nmbr in (1002, 1013,998)
			and (UPPER(t1.str_GL_Charges_Desc) like '%TRAVELFLE%' or UPPER(t1.str_GL_Charges_Desc) like '%TRAVEL FLE%')
			) then 'TravelFLEX' 
			
			when UPPER(t1.str_GL_Charges_Desc) like '%NAME%' then 'Name Change' -- OK
			
			when UPPER(t1.str_GL_Charges_Desc) like '%GROUP BOOKING%' then 'Group Booking Fee' -- OK
			
			when t1.lng_GL_Charge_Type_Id_Nmbr = 6 OR UPPER(t1.str_gl_charges_desc) like '%CHANGE FEE%' OR UPPER(t1.str_gl_charges_desc) like 'CHANGE FEE%' OR t1.lng_GL_Charge_Type_Id_Nmbr = 1013 OR UPPER(t1.str_gl_charges_desc) like '%MODIFICATION%' OR UPPER(t1.str_gl_charges_desc) like 'MODIFICATION%' then 'Modification Fee' -- OK

            when t1.lng_GL_Charge_Type_Id_Nmbr = 2 then 'Airport Bags' -- OK
			
			when t1.lng_GL_Charge_Type_Id_Nmbr in (1009, 1010, 1012, 1016, 1018, 1019, 1020, 1021, 1022, 1023,1024,1025,1026,1027,1029,1030,1031,1032,1033) then t8.str_GL_Charge_Type_Desc
            when t1.lng_GL_Charge_Type_Id_Nmbr = 1002 and t8.str_GL_Charge_Type_Desc = 'SSR' and "Net Charge" = 0 then 'SSR'
			else 'Unassigned'          
end as "Ancillary Category",
case 
	when "Category" in ('Base', 'FlowThru', 'Unassigned') then 0
	when t1.mny_GL_Charges_Amount < 0 then -1 
	when (t1.mny_GL_Charges_Amount = 0 and t1.bit_Fully_Paid =1) then 1 
	when (t1.mny_GL_Charges_Amount = 0 and t1.bit_Fully_Paid !=1) then 0
	when t1.mny_GL_Charges_Amount > 1 then 1
	else 0 
end as "Purchase Cnt",
case 
    when "Category" = 'Base' then round(t1.MNY_GL_CHARGES_AMOUNT-t1.mny_GL_Charges_Discount,2)
    else 0
end as "Base NetCharge",
case 
    when "Category" = 'Ancillary' then round(t1.MNY_GL_CHARGES_AMOUNT-t1.mny_GL_Charges_Discount,2)
    else 0
end as "Ancillary NetCharge",
CASE 
    WHEN "Ancillary Category" IN ('Fare') and "Base NetCharge" >0 then 1 
    else 0
end as "Ticket Purchase",
CASE 
    WHEN  t2.STR_LEG_STATUS = 'X' and "Base NetCharge" <0 then 1 
    else 0
end as "Ticket Refund",
t6.STR_IDENT||'-'||t7.STR_IDENT AS "Route Pair",
t12.STR_COUNTRY_IDENT as "Country",
CASE 
	 WHEN t12.str_country_desc = 'Canada' AND t13.str_country_desc = 'Canada' and t5.mny_Distance < 800 THEN 'Short Haul'
	 WHEN t12.str_country_desc = 'Canada' AND t13.str_country_desc = 'Canada' and t5.mny_Distance > 800 AND t5.mny_Distance < 1600 THEN 'Mid Stage'
	 WHEN t12.str_country_desc = 'Canada' AND t13.str_country_desc = 'Canada' and t5.mny_Distance > 1600 and t5.mny_Distance < 9000  THEN 'Long Haul'
     WHEN t5.LNG_DEP_AIRPORT_ID_NMBR in (18,55,57,59,65,66) or t5.LNG_ARR_AIRPORT_ID_NMBR in (18,55,57,59,65,66) then 'International'
     ELSE 'Sun'
END AS "Classification",
t5.MNY_DISTANCE AS "Kilometers",
round(t1.MNY_EXCHANGE_RATE*100,2) as "Exchange_Rate",
--round(div0(t5.mny_Distance,t17.total_mny_Distance)*100,1) AS "Exchange Rate",
round(t1.MNY_GL_CURRENCY_CHARGES_AMOUNT,2) as "Base Charge",
round(t1.MNY_GL_CURRENCY_CHARGES_DISCOUNT,2) as "Base Discount",
round(t1.MNY_GL_CURRENCY_CHARGES_TAXES,2) as "Base Taxes"

from {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGES') }} t1
left join  {{ source('PSS_AMELIARES_DBO', 'TBL_RES_LEGS') }} t2 on t2.LNG_RES_LEGS_ID_NMBR=t1.LNG_RES_LEGS_ID_NMBR and t2._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_RES_HEADER') }} t3 on t3.LNG_RESERVATION_NMBR=t1.LNG_RESERVATION_NMBR and t3._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_RES_SEGMENTS') }} t4 on t4.LNG_RES_LEGS_ID_NMBR = t2.LNG_RES_LEGS_ID_NMBR  and t4._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_DETAIL') }} t5 on t5.LNG_SKED_DETAIL_ID_NMBR = t4.LNG_SKED_DETAIL_ID_NMBR and t5._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} t6 on t6.LNG_AIRPORT_ID_NMBR = t5.LNG_DEP_AIRPORT_ID_NMBR and t6._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} t7 on t7.LNG_AIRPORT_ID_NMBR = t5.LNG_ARR_AIRPORT_ID_NMBR and t7._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGE_TYPE_DEFINITION') }}  t8 on t8.LNG_GL_CHARGE_TYPE_ID_NMBR=t1.LNG_GL_CHARGE_TYPE_ID_NMBR and t8._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_CURRENCY') }} t9 on t9.LNG_CURRENCY_ID_NMBR = t1.LNG_CURRENCY_ID_NMBR and t9._FIVETRAN_DELETED = 'FALSE'
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_PROVINCE_DEFINITION') }}  AS t10 ON t10.lng_Province_Id_Nmbr = t6.lng_Province_Id_Nmbr and t10._FIVETRAN_DELETED = 'FALSE'
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_PROVINCE_DEFINITION') }}  AS t11 ON t11.lng_Province_Id_Nmbr = t7.lng_Province_Id_Nmbr and t11._FIVETRAN_DELETED = 'FALSE'
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_COUNTRY') }} AS t12 ON t12.LNG_COUNTRY_ID_NMBR = t10.LNG_COUNTRY_ID_NMBR and t12._FIVETRAN_DELETED = 'FALSE'
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_COUNTRY') }} AS t13 ON t13.LNG_COUNTRY_ID_NMBR = t11.LNG_COUNTRY_ID_NMBR and t13._FIVETRAN_DELETED = 'FALSE'
LEFT join {{ source('PSS_AMELIARES_DBO', 'TBL_AGENCY') }} t14 on t14.LNG_AGENCY_ID_NMBR = t3.LNG_AGENCY_ID_NMBR and t14._FIVETRAN_DELETED = 'FALSE'
left join {{ source('PSS_AMELIARES_DBO', 'TBL_TAX_CONFIGURATION') }} t15 on t15.LNG_TAX_CONFIGURATION_ID_NMBR = t1.LNG_TAX_CONFIGURATION_ID_NMBR and t15._FIVETRAN_DELETED = 'FALSE'
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_USERS') }}  AS t16 ON t16.lng_User_Id_Nmbr = t1.lng_last_mod_user_id_nmbr and t16._FIVETRAN_DELETED = 'FALSE'
left join mny_sum t17 on t17.lng_GL_Charges_Id_Nmbr=t1.lng_GL_Charges_Id_Nmbr