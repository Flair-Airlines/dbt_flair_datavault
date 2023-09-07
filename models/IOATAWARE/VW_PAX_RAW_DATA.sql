{{ config(materialized="view") }}

select distinct
    t1.lng_reservation_nmbr as "lng Reservation Nmbr",
    t4.lng_sked_detail_id_nmbr as "lng Sked Detail Id Nmbr",
    t11.str_last_name
    || t11.str_first_name
    || to_varchar(to_date(t11.dtm_dob_date), 'YYYYMMDD') as "Str Index",
    -- change to local flight date
    to_varchar(to_date(t4.dtm_local_etd_date), 'MM/DD/YYYY') as "Flight Date",
    t5.str_ident as "Departure",
    TO_VARCHAR(convert_timezone('UTC', 'America/Denver', t4.dtm_flight_date),'MM/DD/YYYY') as "Actual Flight Date",
    t6.str_ident as "Arrival",
    t1.lng_res_legs_id_nmbr as "lng Res Legs Id Nmbr",
    to_varchar(
        t7.dtm_local_dep_date, 'MM/DD/YYYY HH12:MI:SS AM'
    ) as "dtm Local Dep Date",
    to_varchar(
        t7.dtm_sked_local_dep_date, 'MM/DD/YYYY HH12:MI:SS AM'
    ) as "dtm Sked Local Dep Date",
    t11.str_first_name as "str First Name",
    case
        when trim(t5.str_ident) = 'ANC'
        then '-8'
        when
            trim(t5.str_ident) in (
                'YQQ',
                'YXX',
                'YLW',
                'YKA',
                'YYJ',
                'YXJ',
                'YVR',
                'YXS',
                'YXY',
                'ONT',
                'PHX',
                'LAS',
                'OAK',
                'BUR',
                'SFO',
                'TUS',
                'LAX',
                'SJD'
            )
        then '-7'
        when
            trim(t5.str_ident) in (
                'YYC',
                'YEG',
                'YXE',
                'YQR',
                'YMM',
                'YZF',
                'YQU',
                'YQL',
                'AZA',
                'DEN',
                'MEX',
                'PVR'
            )
        then '-6'
        when
            trim(t5.str_ident)
            in ('YWG', 'YTH', 'YBR', 'MDW', 'MCI', 'BNA', 'ORD', 'KIN', 'CUN')
        then '-5'
        when
            trim(t5.str_ident) in (
                'YHM',
                'YOW',
                'YQG',
                'YQB',
                'YYZ',
                'YXU',
                'YUL',
                'YKF',
                'YQT',
                'PIE',
                'RSW',
                'SFB',
                'SRQ',
                'MLB',
                'TPA',
                'MIA',
                'MCO',
                'FLL',
                'ATL',
                'JFK',
                'PUJ'
            )
        then '-4'
        when trim(t5.str_ident) in ('YQM', 'YHZ', 'YYG', 'YSJ', 'ZBF')
        then '-3'
        when trim(t5.str_ident) in ('YDF', 'YYT')
        then '-2.5'
        when trim(t5.str_ident) = 'LRT'
        then '+2'
        when trim(t5.str_ident) = 'SRY'
        then '+3.5'
        when trim(t5.str_ident) = 'BNE'
        then '+10'
        else 'unassigned'
    end as "Time Zone Adjustment",
    t11.str_last_name as "str Last Name",
    t3.str_res_status as "str Res Status",
    t3.str_res_checkin_boarded as "str Res Check In Boarded",
    case
        when
            t3.str_res_status = 'C'
            and t3.str_res_checkin_boarded = 'B'
            and t7.str_flight_status = 'C'
        then 'Boarded'
        when
            t3.str_res_status = 'C'
            and t3.str_res_checkin_boarded = 'B'
            and t7.str_flight_status = 'Y'
        then 'Boarded'
        when
            t3.str_res_status = 'X'
            and t3.str_res_checkin_boarded = 'N'
            and t7.str_flight_status = 'C'
        then 'Cancelled'
        when
            t3.str_res_status = 'X'
            and t3.str_res_checkin_boarded = 'N'
            and t7.str_flight_status = 'X'
        then 'Flight Cancelled'
        when
            t3.str_res_status = 'C'
            and t3.str_res_checkin_boarded = 'N'
            and t7.str_flight_status = 'C'
        then 'Confirmed'
        when
            t3.str_res_status = 'X'
            and t3.str_res_checkin_boarded = 'X'
            and t7.str_flight_status = 'C'
        then 'No-Show'
        when
            t3.str_res_status = 'C'
            and t3.str_res_checkin_boarded = 'C'
            and t7.str_flight_status = 'C'
        then 'Checked-in'
        else 'Other'
    end as "PAXStatus",
    t4.str_flight_nmbr as "str Flight Nmbr",
    t2.lng_pax_id_nmbr as "lng Pax Id Nmbr",
    case
        when t1.bit_fully_paid = 'FALSE' and t1.mny_gl_charges_amount = 0 then 1 else 0
    end as "NRPS",
    t12.lng_leg_nmbr as "lng Leg Nmbr",
    to_varchar(t11.dtm_dob_date, 'MM/DD/YYYY HH12:MI:SS AM') as "dtm DOB Date",
    case
        when t2.lng_pax_type_codes_id_nmbr = 6 and t9.lng_agency_id_nmbr = 1
        then 1
        else 0
    end as "NRSA",
    t2.LNG_CREATION_USER_ID_NMBR as "lng Creation User Id Nmbr",
    t11.str_gender as "str Gender",
    t7.str_flight_status as "str Flight Status",
    t3.lng_res_segments_id_nmbr as "lng Res Segments Id Nmbr",
    case when t3.str_res_thru_segment = 'Y' then 1 else 0 end as "Connecting",
    t2.lng_pax_type_codes_id_nmbr as "lng Pax Type Codes Id Nmbr",
    case
        when "PAXStatus" = 'Boarded'
        then 0
        when "PAXStatus" = 'Cancelled'
        then 0
        when "PAXStatus" = 'Flight Cancelled'
        then 0
        when "PAXStatus" = 'Confirmed'
        then 0
        when "PAXStatus" = 'No-show' and "NRPS" = 1 or "NRSA" = 1
        then 0
        else 1
    end as "No Show PAX",
    t9.lng_agency_id_nmbr as "lng Agency Id Nmbr",
    to_varchar(
        convert_timezone('UTC', 'America/Denver', t1.dtm_gl_charges_date), 'MM/DD/YYYY'
    ) as "Sales Date",
    -- TO_VARCHAR(TO_DATE(t1.DTM_GL_CHARGES_DATE),'YYYY/MM/DD') as "Sales Date",
    case
        when t3.str_res_status = 'C' and t1.bit_fully_paid = 'TRUE' then 1 else 0
    end as "Valid PAX",
    case when t2.lng_pax_type_codes_id_nmbr = 2 then 1 else 0 end as "Child",
    case when  t1.MNY_GL_CURRENCY_CHARGES_TOTAL <= 0.001  then 0 else 1 end as "PAX Rev",
    t8.str_ref1 as "PNR",
    t9.str_agency_name as "str Agency Name",
    case when  t1.MNY_GL_CURRENCY_CHARGES_TOTAL <= 0.001 then 0 else 1 end as "Booked PAX",
    case
        when t16.str_country_desc = 'Canada' and t17.str_country_desc = 'Canada'
        then 0
        else 1
    end as "International",
    /*case 
    when "Valid PAX" = 1 then 1
    else 0
end as "Flown PAX",*/
    case
        when
            t1.lng_gl_charge_type_id_nmbr in (3, 999, 995)
            or t3.str_res_status = 'C'
            or t3.str_res_checkin_boarded = 'B'
        then 1
        else 0
    end as "Flown PAX",
    t3.str_res_thru_segment as "str Res Thru Segment",
    t7.str_tail_number_identifier as "str Tail Number Identifier",
    t3.str_boarding_pass_nmbr as "str Boarding Pass Nmbr",
    '#Error' as "tsp Timestamp",
    to_varchar(t3.dtm_last_mod_date, 'MM/DD/YYYY HH12:MI:SS AM') as "dtm Last Mod Date",
    -- t8.STR_CONTACT_EMAIL as "str Email",
    -- t8.STR_CONTACT_TEL as "str Home Tel",
    t11.str_email as "str email",
    t11.str_home_tel as "strHomeTel",
    -- t11.STR_GENERALNUMBER1 as "str General Number1",
    -- t11.STR_GENERALNUMBER2 as "str General   Number2",
    t8.lng_cax_num as "lng Cax Num",
    -- t11.STR_ADD1 as "str Addr1",
    -- t11.STR_ADD2 as "str Addr2",
    t11.str_city as "str City",
    t11.str_postal_code as "str Postal Code",
    t3.str_special_needs as "str Special Needs",
    t13.str_user_logon_name as "str User Logon Name",
    t10.str_seat_ident as "str Seat Ident",
    t3.lng_last_mod_user_id_nmbr as "lng Last Mod User Id   Nmbr",
    t18.str_user_name as "Check In Agent",
    1 as "Local",
    to_varchar(
        t10.dtm_boarding_time, 'MM/DD/YYYY HH12:MI:SS AM'
    ) as "dtm Boarding Time",
    t10.str_infant_pass_sequence_number as "str Infant Pass Sequence Number",
    to_varchar(
        t10.dtm_iatci_action_timeout, 'MM/DD/YYYY HH12:MI:SS AM'
    ) as "Check In Time",

    case
        when
            trim(t10.str_seat_ident) <> ''
            and trim(t10.str_infant_pass_sequence_number) = ''
            and "Flown PAX" = 1
        then 1
        when
            trim(t10.str_seat_ident) <> ''
            and trim(t10.str_infant_pass_sequence_number) <> ''
            and "Flown PAX" = 1
        then 2
        when
            trim(t10.str_seat_ident) = ''
            and trim(t10.str_infant_pass_sequence_number) = ''
            and "Flown PAX" = 1
        then 1
        else 0
    end as "On Plane",
    case when t18.str_user_name = 'ABBAPI' then 1 else 0 end as "Online Checked In",
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
        when
            trim(t10.str_scheduled_rbd) in ('T', 'S', 'U')
            or trim(t10.str_checkin_status) != 'CHECKED_IN'
        then 0
        else 1
    end as "Airport Checked In",
    case
        when trim(t10.str_infant_pass_sequence_number) <> '' then 1 else 0
    end as "Infant",
    t10.str_checkin_status as "str Check In Status",
    to_varchar(t10.dtm_last_mod_date, 'MM/DD/YYYY HH12:MI:SS AM') as "Board Time"
from {{ source("PSS_AMELIARES_DBO", "TBL_GL_CHARGES") }} t1
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_RES_LEGS") }} t19
    on t19.lng_res_legs_id_nmbr = t1.lng_res_legs_id_nmbr
    and t19._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_RES_PAX_GROUP") }} t2
    on t2.lng_res_pax_group_id_nmbr = t19.lng_res_pax_group_id_nmbr
    and t2._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_RES_SEGMENTS") }} t3
    on t3.lng_res_legs_id_nmbr = t19.lng_res_legs_id_nmbr
    and t3._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_SKED_DETAIL") }} t4
    on t4.lng_sked_detail_id_nmbr = t3.lng_sked_detail_id_nmbr
    and t4._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_AIRPORT") }} t5
    on t5.lng_airport_id_nmbr = t4.lng_dep_airport_id_nmbr
    and t5._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_AIRPORT") }} t6
    on t6.lng_airport_id_nmbr = t4.lng_arr_airport_id_nmbr
    and t6._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_SKED_FLIGHT_WATCH_JOURNEY_LOG") }} t7
    on t7.lng_sked_detail_id_nmbr = t4.lng_sked_detail_id_nmbr
    and t7._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_RES_HEADER") }} t8
    on t8.lng_reservation_nmbr = t1.lng_reservation_nmbr
    and t8._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_AGENCY") }} t9
    on t9.lng_agency_id_nmbr = t8.lng_agency_id_nmbr
    and t9._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_RES_CHECKIN") }} t10
    on t10.lng_res_legs_id_nmbr = t1.lng_res_legs_id_nmbr
    and t10._fivetran_deleted = false
left join
    {{ source("AMELIA_DBO", "STG_PAX") }} t11
    on t11.lng_pax_id_nmbr = t2.lng_pax_id_nmbr
    AND ACTIVE_FLAG = 'Y'
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_RES_LEGS") }} t12
    on t12.lng_res_legs_id_nmbr = t1.lng_res_legs_id_nmbr
    and t12._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_USERS") }} t13
    on t1.lng_creation_user_id_nmbr = t13.lng_user_id_nmbr
    and t13._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_PROVINCE_DEFINITION") }} as t14
    on t5.lng_province_id_nmbr = t14.lng_province_id_nmbr
    and t14._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_PROVINCE_DEFINITION") }} as t15
    on t6.lng_province_id_nmbr = t15.lng_province_id_nmbr
    and t15._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_COUNTRY") }} as t16
    on t16.lng_country_id_nmbr = t14.lng_country_id_nmbr
    and t16._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_COUNTRY") }} as t17
    on t17.lng_country_id_nmbr = t15.lng_country_id_nmbr
    and t17._fivetran_deleted = false
left join
    {{ source("PSS_AMELIARES_DBO", "TBL_USERS") }} t18
    on t10.lng_creation_user_id_nmbr = t18.lng_user_id_nmbr
    and t18._fivetran_deleted = false
