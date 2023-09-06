{{
    config(
        materialized = 'view',
    )
}}

with tmp as (select
CASE
    WHEN t9.STR_AGENCY_NAME <> '' AND t22.STR_USER_LOGON_NAME <> '' then 'Amelia'
    else 'MISChoice'
end as "Source",
        --t12.dtm_etd as FlightDate_UTC,
        TO_VARCHAR(TO_DATE(t12.dtm_local_etd_date),'MM/DD/YYYY') as "Flight Date",
		TO_VARCHAR(TO_TIME(t12.dtm_local_etd_date),'HH24:MI:SS') as "Flight time",
		t1.lng_Reservation_Nmbr as "Reservation Nmbr",
		t12.lng_Sked_Detail_Id_Nmbr as "Sked Detail Id Nmbr",
		to_varchar(t1.dtm_GL_Charges_Date,'MM/DD/YYYY HH12:MI:SS AM') as "Charge Date",
        t5.str_Ident as "Departure",
        t6.str_Ident as "Arrival",
       t12.str_Flight_Nmbr as "Flight Nmbr",
       t1.lng_Res_Legs_Id_Nmbr as "Legs Id Nmbr",
       t1.lng_GL_Charge_Type_Id_Nmbr as "Charge Type",
       t1.mny_GL_Charges_Amount - t1.mny_GL_Charges_Discount as NetCharge,
       t1.mny_GL_Charges_Taxes,
       t1.mny_GL_Charges_Total,
       t1.str_GL_Charges_Desc as "Charges Desc",
       --1 as _Percent,
       t2.str_GL_Charge_Type_Desc as "Charge Type Desc",
       t1.lng_Creation_User_Id_Nmbr as "User Id Nmbr",
        t4.str_Leg_Status as "Leg Status",
       t4.lng_Leg_Nmbr as "Leg Nmbr",
       t8.lng_Agency_Id_Nmbr as "Agency Id Nmbr",
       t9.str_Agency_Name as "Agency Name",
       t8.str_Ref1 as "Reference",
       t7.str_Currency_Ident as "Currency Ident",
       t1.mny_Exchange_Rate as "Exchange Rate",
      t12.mny_Distance,
       
		case 
			when (t1.lng_GL_Charge_Type_Id_Nmbr = 4 OR (t1.lng_gl_charge_type_id_nmbr = 1028 AND STR_GL_CHARGES_DESC like '%Fare Rebate%')) then 'Base'
			when t1.lng_GL_Charge_Type_Id_Nmbr in (2, 3, 6, 995, 996, 999, 1000, 1001, 1002, 1013,1005) then 'Ancillary'
			when t1.lng_GL_Charge_Type_Id_Nmbr in (1, 5, 1009, 1010, 1012, 1016, 1018, 1019, 1020, 1021, 1022, 1023,1024,1025,1026,1027,1029,1030,1031,1032,1033) then 'FlowThru'
			else 'Unassigned'
		end as "Category",
		case 
			when t1.lng_GL_Charge_Type_Id_Nmbr = 1 then 'Airport Improvement Fee'
			when t1.lng_GL_Charge_Type_Id_Nmbr = 3 then 'Cancellation' 
			when t1.lng_GL_Charge_Type_Id_Nmbr = 4 OR (t1.lng_gl_charge_type_id_nmbr = 1028 AND STR_GL_CHARGES_DESC like '%Fare Rebate%') then 'Fare'
			when t1.lng_GL_Charge_Type_Id_Nmbr = 5 then 'Air Traveller Security Charge'

			when t1.lng_GL_Charge_Type_Id_Nmbr = 995 OR (UPPER(t1.str_GL_Charges_Desc)) like '%NO SHOW%' then 'No Show'
			when t1.lng_GL_Charge_Type_Id_Nmbr IN (999,1005) AND (UPPER(t1.str_GL_Charges_Desc) like '%NON REFUNDABLE%' OR UPPER(t1.str_GL_Charges_Desc) like '%NON-REFUNDABLE%') then 'Non Refundable' -- OK
			when t1.lng_GL_Charge_Type_Id_Nmbr = 1000 then 'Call Center Fee'
			when t1.lng_GL_Charge_Type_Id_Nmbr = 1001 then 'Seat Assignment' 
			
			when t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002)
			and (UPPER(t1.str_GL_Charges_Desc) like '%ACF%'
            OR  t1.str_GL_Charges_Desc like '%Airport Check-in%'
			) then 'ACF - Prepaid'
			
			when (UPPER(t1.str_GL_Charges_Desc) like '%GATE%'
			) then 'Carry On - Gate' 
			
			when (
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002,1005)
			and UPPER(t1.str_GL_Charges_Desc) like '%CARRY ON%'
			) then 'Carry On' 
			
			when (
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002)
			and (UPPER(t1.str_GL_Charges_Desc) like '%CHK BAG%' 
--		
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
			
			when (
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
			
			when (
			t1.lng_GL_Charge_Type_Id_Nmbr in (2, 1002) 
			and (UPPER(t1.str_GL_Charges_Desc) like '%OVERWEIGHT%' 
				or UPPER(t1.str_GL_Charges_Desc) like '%OVERSIZE%'
				or UPPER(t1.str_GL_Charges_Desc) like '%EXCESS%')
			) then 'Misc Bag Charges' 
			
			
			when (
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
			
			when t1.lng_GL_Charge_Type_Id_Nmbr in (1009, 1010, 1012, 1016, 1018, 1019, 1020, 1021, 1022, 1023,1024,1025,1026,1027,1029,1030,1031,1032,1033) then t2.str_GL_Charge_Type_Desc
            when t1.lng_GL_Charge_Type_Id_Nmbr = 1002 and t2.str_GL_Charge_Type_Desc = 'SSR' and NetCharge = 0 then 'SSR'
			else 'Unassigned'          
		end as "AncillaryCategory",
		case when t8.lng_Agency_Id_Nmbr in ('1','8','275') then 'Direct'
		else 'InDirect' end as "Channel",
       bit_Fully_Paid,
       t11.lng_Res_Segments_Id_Nmbr as "Segments Id Nmbr",
       sum(mny_Distance) OVER (PARTITION by t1.lng_GL_Charges_Id_Nmbr) as total_mny_Distance,
case 
    WHEN t23.str_country_desc = 'Canada' AND t24.str_country_desc = 'Canada' THEN 'Domestic'
    ELSE 'International'
end as "Transborder",
CASE 
	 WHEN t23.str_country_desc = 'Canada' AND t24.str_country_desc = 'Canada' and t12.mny_Distance < 800 THEN 'Short Haul'
	 WHEN t23.str_country_desc = 'Canada' AND t24.str_country_desc = 'Canada' and t12.mny_Distance > 800 AND t12.mny_Distance < 1600 THEN 'Mid Stage'
	 WHEN t23.str_country_desc = 'Canada' AND t24.str_country_desc = 'Canada' and t12.mny_Distance > 1600 and t12.mny_Distance < 9000  THEN 'Long Haul'
     WHEN t12.LNG_DEP_AIRPORT_ID_NMBR in (18,55,57,59,65,66) or t12.LNG_ARR_AIRPORT_ID_NMBR in (18,55,57,59,65,66) then 'International'
     ELSE 'Sun'
END AS "Classification",
case 
	when "Category"in ('Base', 'FlowThru', 'Unassigned') then 0
	when t1.mny_GL_Charges_Amount < 0 then -1 
	when (t1.mny_GL_Charges_Amount = 0 and bit_Fully_Paid =1) then 1 
	when (t1.mny_GL_Charges_Amount = 0 and bit_Fully_Paid !=1) then 0 
	when t1.mny_GL_Charges_Amount > 1 then 1
	else 0 
end as "PurchaseCnt"

FROM {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGES') }}  AS t1

LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGE_TYPE_DEFINITION') }} AS t2 ON t1.lng_GL_Charge_Type_Id_Nmbr = t2.lng_GL_Charge_Type_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_CHARGE_CATEGORY') }} AS t3 ON t2.lng_Charge_Category_Id_Nmbr = t3.lng_Charge_Category_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_LEGS') }} AS t4 ON t1.lng_Res_Legs_Id_Nmbr = t4.lng_Res_Legs_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} AS t5 ON t4.lng_Dep_Airport_Id_Nmbr = t5.lng_Airport_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} AS t6 ON t4.lng_Arr_Airport_Id_Nmbr = t6.lng_Airport_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_CURRENCY') }} AS t7 ON t1.lng_Currency_Id_Nmbr = t7.lng_Currency_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_HEADER') }}  AS t8 ON t1.lng_Reservation_Nmbr = t8.lng_Reservation_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_AGENCY') }} AS t9 ON t8.lng_Agency_Id_Nmbr = t9.lng_Agency_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_SEGMENTS') }} AS t11 ON t1.lng_Res_Legs_Id_Nmbr = t11.lng_Res_Legs_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_DETAIL') }}  AS t12 ON t11.lng_Sked_Detail_id_Nmbr = t12.lng_Sked_Detail_id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_TAX_CONFIGURATION') }}  AS t15 ON t1.lng_Tax_Configuration_Id_Nmbr = t15.lng_Tax_Configuration_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} AS t16 ON t12.lng_Dep_Airport_Id_Nmbr = t16.lng_Airport_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} AS t17 ON t12.lng_Arr_Airport_Id_Nmbr = t17.lng_Airport_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_PROVINCE_DEFINITION') }} AS t18 ON t16.lng_Province_Id_Nmbr = t18.lng_Province_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_PROVINCE_DEFINITION') }} AS t19 ON t17.lng_Province_Id_Nmbr = t19.lng_Province_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_USERS') }} AS t22 ON t1.lng_Creation_User_Id_Nmbr  = t22.lng_User_Id_Nmbr
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_COUNTRY') }} AS t23 ON t23.LNG_COUNTRY_ID_NMBR = t18.LNG_COUNTRY_ID_NMBR 
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_COUNTRY') }} AS t24 ON t24.LNG_COUNTRY_ID_NMBR = t19.LNG_COUNTRY_ID_NMBR 

), final as (select 

		"Source",
		"Flight Date",
        "Flight time",
		"Reservation Nmbr",
		"Sked Detail Id Nmbr",
		"Charge Date",
       "Departure",
       "Arrival",
       "Legs Id Nmbr",
       "Charge Type",
       round(NetCharge * div0(mny_Distance,total_mny_Distance),2) as "Net Charge",
       round(mny_GL_Charges_Taxes * div0(mny_Distance,total_mny_Distance),2) as "Taxes",
       round(mny_GL_Charges_Total * div0(mny_Distance,total_mny_Distance),2) as "Total Charge",
       "Charge Type Desc",
       "Flight Nmbr",
       round(div0(mny_Distance,total_mny_Distance)*100,1) as "Percent of Full Leg",
       "Charges Desc",
       	"User Id Nmbr",
       "Leg Nmbr",
      "Segments Id Nmbr",
       "Agency Id Nmbr",
       "Reference",
       "Currency Ident",
       "Exchange Rate",
       "Agency Name",
       "Sale_Username",
        "Transborder",
      "Leg Status",
       --mny_Distance,
		"Category",
		"AncillaryCategory",
        "PurchaseCnt",
        "Classification",
		"Channel"
from tmp)
select * from final
--where date("Flight Date")= '2023-05-01' ;