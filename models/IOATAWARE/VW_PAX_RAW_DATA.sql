{{
    config(
        materialized = 'view'
    )
}}

select distinct
t1.LNG_RESERVATION_NMBR as "lng Reservation Nmbr",
t4.LNG_SKED_DETAIL_ID_NMBR as "lng Sked Detail Id Nmbr",
 t11.STR_LAST_NAME ||t11.STR_FIRST_NAME || TO_VARCHAR(TO_DATE(t11.DTM_DOB_DATE ),'YYYYMMDD') as "Str Index",
 --change to local flight date
TO_VARCHAR(TO_DATE(t4.DTM_LOCAL_ETD_DATE),'MM/DD/YYYY') as "Flight Date",
t5.STR_IDENT as "Departure",
TO_VARCHAR(TO_DATE(t4.DTM_FLIGHT_DATE),'MM/DD/YYYY') as "Actual Flight Date",
t6.STR_IDENT as "Arrival",
t1.LNG_RES_LEGS_ID_NMBR as "lng Res Legs Id Nmbr",
to_varchar(t7.DTM_LOCAL_DEP_DATE,'MM/DD/YYYY HH12:MI:SS AM') as "dtm Local Dep Date",
to_varchar(t7.DTM_SKED_LOCAL_DEP_DATE,'MM/DD/YYYY HH12:MI:SS AM') as "dtm Sked Local Dep Date",
t11.STR_FIRST_NAME as "str First Name",   
case 
    when trim(t5.STR_IDENT)  = 'ANC' then '-8'
    when trim(t5.STR_IDENT) in ('YQQ','YXX','YLW','YKA','YYJ','YXJ','YVR','YXS','YXY','ONT','PHX','LAS','OAK','BUR','SFO','TUS','LAX','SJD') then '-7'
    when trim(t5.STR_IDENT) in ('YYC','YEG','YXE','YQR','YMM','YZF','YQU','YQL','AZA','DEN','MEX','PVR') then '-6'
    when trim(t5.STR_IDENT) in ('YWG','YTH','YBR','MDW','MCI','BNA','ORD','KIN','CUN') then '-5'
    when trim(t5.STR_IDENT) in ('YHM','YOW','YQG','YQB','YYZ','YXU','YUL','YKF','YQT','PIE','RSW','SFB','SRQ','MLB','TPA','MIA','MCO','FLL','ATL','JFK','PUJ') then '-4'
    when trim(t5.STR_IDENT) in ('YQM','YHZ','YYG','YSJ','ZBF') then '-3'
    when trim(t5.STR_IDENT) in ('YDF','YYT') then '-2.5'
    when trim(t5.STR_IDENT) = 'LRT' then '+2'
    when trim(t5.STR_IDENT) = 'SRY' then '+3.5'
    when trim(t5.STR_IDENT) = 'BNE' then '+10'
    else 'unassigned'
end as "Time Zone Adjustment",
t11.STR_LAST_NAME as "str Last Name",
t3.STR_RES_STATUS as "str Res Status",
t3.STR_RES_CHECKIN_BOARDED as "str Res Check In Boarded",
case 
    when t3.STR_RES_STATUS = 'C' and t3.STR_RES_CHECKIN_BOARDED = 'B' and t7.STR_FLIGHT_STATUS ='C' then 'Boarded'
    when t3.STR_RES_STATUS = 'C' and t3.STR_RES_CHECKIN_BOARDED = 'B' and t7.STR_FLIGHT_STATUS ='Y' then 'Boarded'
    when t3.STR_RES_STATUS = 'X' and t3.STR_RES_CHECKIN_BOARDED = 'N' and t7.STR_FLIGHT_STATUS ='C' then 'Cancelled'
    when t3.STR_RES_STATUS = 'X' and t3.STR_RES_CHECKIN_BOARDED = 'N' and t7.STR_FLIGHT_STATUS ='X' then 'Flight Cancelled'
    when t3.STR_RES_STATUS = 'C' and t3.STR_RES_CHECKIN_BOARDED = 'N' and t7.STR_FLIGHT_STATUS ='C' then 'Confirmed'
    when t3.STR_RES_STATUS = 'X' and t3.STR_RES_CHECKIN_BOARDED = 'X' and t7.STR_FLIGHT_STATUS ='C' then 'No-Show'
    when t3.STR_RES_STATUS = 'C' and t3.STR_RES_CHECKIN_BOARDED = 'C' and t7.STR_FLIGHT_STATUS ='C' then 'Checked-in'
    else 'Other'
end as "PAXStatus",
t4.STR_FLIGHT_NMBR as "str Flight Nmbr",
t2.LNG_PAX_ID_NMBR as "lng Pax Id Nmbr",
CASE 
    WHEN t1.BIT_FULLY_PAID = 'FALSE' AND t1.MNY_GL_CHARGES_AMOUNT = 0 then 1
    else 0
end as "NRPS",
t12.LNG_LEG_NMBR as "lng Leg Nmbr",
to_varchar(t11.DTM_DOB_DATE,'MM/DD/YYYY HH12:MI:SS AM') as "dtm DOB Date",
case 
    when t2.LNG_PAX_TYPE_CODES_ID_NMBR = 6 and t9.LNG_AGENCY_ID_NMBR = 1 then 1
    else 0
end as "NRSA",
t1.LNG_CREATION_USER_ID_NMBR as "lng Creation User Id Nmbr",
t11.STR_GENDER as "str Gender",
t7.STR_FLIGHT_STATUS as "str Flight Status",
t3.LNG_RES_SEGMENTS_ID_NMBR as "lng Res Segments Id Nmbr",
case 
    when t3.STR_RES_THRU_SEGMENT = 'Y' then 1
    else 0
end as "Connecting",
t2.LNG_PAX_TYPE_CODES_ID_NMBR as "lng Pax Type Codes Id Nmbr",
case
    when "PAXStatus" = 'Boarded' then 0
    when "PAXStatus" = 'Cancelled' then 0
    when "PAXStatus" = 'Flight Cancelled' then 0
    when "PAXStatus" = 'Confirmed' then 0
    when "PAXStatus" = 'No-show' and "NRPS" = 1 or "NRSA" = 1 then 0
    else 1
end as "No Show PAX",
t9.LNG_AGENCY_ID_NMBR as "lng Agency Id Nmbr",
 TO_VARCHAR(convert_timezone('UTC', 'America/Denver', t1.dtm_GL_Charges_Date),'MM/DD/YYYY') as "Sales Date",
--TO_VARCHAR(TO_DATE(t1.DTM_GL_CHARGES_DATE),'YYYY/MM/DD') as "Sales Date",

case
    when t3.STR_RES_STATUS = 'C' and  t1.BIT_FULLY_PAID = 'TRUE' THEN 1
    else 0
end as "Valid PAX",
case 
    when t2.LNG_PAX_TYPE_CODES_ID_NMBR = 2 then 1
    else 0
end as "Child",
case
    when t1.MNY_GL_CURRENCY_CHARGES_TOTAL = 0 then 0
    else 1
end as "PAX Rev",
T8.STR_REF1 as "PNR",
t9.STR_AGENCY_NAME as "str Agency Name",
case
    when t1.MNY_GL_CURRENCY_CHARGES_TOTAL = 0 then 0
    else 1
end as "Booked PAX",
CASE 
	 WHEN t16.str_country_desc = 'Canada' AND t17.str_country_desc = 'Canada'  THEN 0
	 ELSE 1
end as "International",
/*case 
    when "Valid PAX" = 1 then 1
    else 0
end as "Flown PAX",*/
case 
    when t1.LNG_GL_CHARGE_TYPE_ID_NMBR in (3,999,995) or t3.STR_RES_STATUS = 'C' or t3.STR_RES_CHECKIN_BOARDED = 'B' then 1
    else 0
end as "Flown PAX",
t3.STR_RES_THRU_SEGMENT as "str Res Thru Segment",
t7.STR_TAIL_NUMBER_IDENTIFIER as "str Tail Number Identifier",
t3.STR_BOARDING_PASS_NMBR as "str Boarding Pass Nmbr",
'#Error' as "tsp Timestamp",
to_varchar(t3.DTM_LAST_MOD_DATE,'MM/DD/YYYY HH12:MI:SS AM') as "dtm Last Mod Date",
t11.STR_EMAIL as "str Email",
t11.STR_HOME_TEL as "strHomeTelephone"
--t11.STR_GENERALNUMBER1 as "str General Number1",
--t11.STR_GENERALNUMBER2 as "str General   Number2",
t8.LNG_CAX_NUM as "lng Cax Num",
--t11.STR_ADD1 as "str Addr1",
--t11.STR_ADD2 as "str Addr2",
t11.STR_CITY as "str City",
t11.STR_POSTAL_CODE as "str Postal Code",
t3.STR_SPECIAL_NEEDS as "str Special Needs",
t13.STR_USER_LOGON_NAME as "str User Logon Name",
t10.STR_SEAT_IDENT as "str Seat Ident",
t3.LNG_LAST_MOD_USER_ID_NMBR as "lng Last Mod User Id   Nmbr",
t18.STR_USER_NAME  as "Check In Agent",
1 AS "Local",
to_varchar(t10.DTM_BOARDING_TIME,'MM/DD/YYYY HH12:MI:SS AM') as "dtm Boarding Time",
t10.STR_INFANT_PASS_SEQUENCE_NUMBER as "str Infant Pass Sequence Number",
to_varchar(t10.DTM_IATCI_ACTION_TIMEOUT ,'MM/DD/YYYY HH12:MI:SS AM') as "Check In Time", 

case
    when TRIM(t10.STR_SEAT_IDENT) <>'' and TRIM(t10.STR_INFANT_PASS_SEQUENCE_NUMBER) ='' and "Flown PAX" = 1 then 1
    when   TRIM(t10.STR_SEAT_IDENT) <>'' and TRIM(t10.STR_INFANT_PASS_SEQUENCE_NUMBER)<>'' and "Flown PAX" = 1 then 2
    when   TRIM(t10.STR_SEAT_IDENT) = '' and TRIM(t10.STR_INFANT_PASS_SEQUENCE_NUMBER) = '' and "Flown PAX" = 1 then 1
    else 0
end as "On Plane",
case 
    when t18.STR_USER_NAME = 'ABBAPI' then 1
    else 0
end as "Online Checked In",
/*case 
    when t10.STR_CHECKIN_STATUS = 'CHECKED_IN' and  TRIM(t10.STR_SEAT_IDENT) <> '' then 1
    when t10.STR_CHECKIN_STATUS = 'CHECKED_IN' and  TRIM(t10.STR_SEAT_IDENT)  = '' then 0
    when t10.STR_CHECKIN_STATUS = 'NOT_CHECKED_IN' and TRIM(t10.STR_SEAT_IDENT) <> '' then 1
    when t10.STR_CHECKIN_STATUS = 'NOT_CHECKED_IN' and TRIM(t10.STR_SEAT_IDENT) = '' then 0
    else 0
end as "Airport Checked In",*/
/*case 
    when t18.STR_USER_NAME = 'ABBAPI' and "No Show PAX"=1 then 0
    else 1
end as "Airport Checked In",*/
case 
    when trim(t10.STR_SCHEDULED_RBD) in ('T','S','U') or trim(t10.STR_CHECKIN_STATUS) != 'CHECKED_IN' then 0
    else 1
end as "Airport Checked In",
case
    when TRIM(t10.STR_INFANT_PASS_SEQUENCE_NUMBER) <> '' then 1
    else 0
end as "Infant",
t10.STR_CHECKIN_STATUS as "str Check In Status",
to_varchar(t10.DTM_LAST_MOD_DATE ,'MM/DD/YYYY HH12:MI:SS AM') as "Board Time"
from {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGES') }} t1
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_LEGS') }} t19 on  t19.LNG_RES_LEGS_ID_NMBR = t1.LNG_RES_LEGS_ID_NMBR and t19._fivetran_deleted = FALSE
LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_PAX_GROUP') }} t2 on t2.LNG_RES_PAX_GROUP_ID_NMBR=t19.LNG_RES_PAX_GROUP_ID_NMBR and t2._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_RES_SEGMENTS') }}  t3 on t3.LNG_RES_LEGS_ID_NMBR = t19.LNG_RES_LEGS_ID_NMBR and  t3._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_DETAIL') }} t4 on t4.LNG_SKED_DETAIL_ID_NMBR = t3.LNG_SKED_DETAIL_ID_NMBR and t4._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} t5 on t5.LNG_AIRPORT_ID_NMBR = t4.LNG_DEP_AIRPORT_ID_NMBR and t5._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_AIRPORT') }} t6 on t6.LNG_AIRPORT_ID_NMBR = t4.LNG_ARR_AIRPORT_ID_NMBR and t6._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_FLIGHT_WATCH_JOURNEY_LOG') }}  t7 on t7.LNG_SKED_DETAIL_ID_NMBR = t4.LNG_SKED_DETAIL_ID_NMBR and t7._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_RES_HEADER') }} t8 on t8.LNG_RESERVATION_NMBR = t1.LNG_RESERVATION_NMBR and t8._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_AGENCY') }} t9 on t9.LNG_AGENCY_ID_NMBR = t8.LNG_AGENCY_ID_NMBR and t9._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_RES_CHECKIN') }} t10 on t10.LNG_RES_LEGS_ID_NMBR = t1.LNG_RES_LEGS_ID_NMBR and t10._fivetran_deleted = FALSE
LEFT JOIN  {{ source('AMELIA_DBO', 'TBL_PAX') }} t11 on t11.LNG_PAX_ID_NMBR = t2.LNG_PAX_ID_NMBR and t11._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_RES_LEGS') }} t12 ON t12.LNG_RES_LEGS_ID_NMBR = t1.LNG_RES_LEGS_ID_NMBR and t12._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_USERS') }} t13 ON t1.LNG_CREATION_USER_ID_NMBR  = t13.lng_User_Id_Nmbr and t13._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_PROVINCE_DEFINITION') }}  AS t14 ON t5.lng_Province_Id_Nmbr = t14.lng_Province_Id_Nmbr and t14._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_PROVINCE_DEFINITION') }} AS t15 ON t6.lng_Province_Id_Nmbr = t15.lng_Province_Id_Nmbr and t15._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_COUNTRY') }}  AS t16 ON t16.LNG_COUNTRY_ID_NMBR = t14.LNG_COUNTRY_ID_NMBR and t16._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_COUNTRY') }} AS t17 ON t17.LNG_COUNTRY_ID_NMBR = t15.LNG_COUNTRY_ID_NMBR and t17._fivetran_deleted = FALSE
LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_USERS') }} t18 ON t10.LNG_CREATION_USER_ID_NMBR  = t18.lng_User_Id_Nmbr and t18._fivetran_deleted = FALSE