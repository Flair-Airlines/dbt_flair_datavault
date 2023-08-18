{{
    config(
        materialized = 'view',
    )
}}
WITH TBL_GL_CHARGES_MAX AS (
 SELECT * FROM 
 (select * 
, ROW_NUMBER() OVER (PARTITION BY lng_Res_Legs_Id_Nmbr ORDER BY dtm_Creation_Date DESC) AS RN
, sum(MNY_GL_CHARGES_AMOUNT) over (PARTITION BY lng_Res_Legs_Id_Nmbr) as total_charge
from {{ source('PSS_AMELIARES_DBO', 'TBL_GL_CHARGES') }}
WHERE "_FIVETRAN_DELETED" = FALSE) AS T
WHERE  T.RN = 1
and total_charge > 0
 )
 
 , T_CHARGES_FARECLASS AS (
SELECT TBL_GL_CHARGES_MAX.lng_Res_Legs_Id_Nmbr
, TBL_GL_CHARGES_MAX.lng_reservation_nmbr
, TBL_GL_CHARGES_MAX.str_GL_Charges_Desc 
, tbl_Fare_Class.lng_Fare_Category_Id_Nmbr 
, tbl_Fare_Class.lng_Fare_Class_Id_Nmbr
, tbl_Fare_Class.str_Fare_Class_Short 
, tbl_Fare_Class.str_Fare_Class_Description 
FROM TBL_GL_CHARGES_MAX AS TBL_GL_CHARGES_MAX
LEFT JOIN (SELECT * FROM {{ source('PSS_AMELIARES_DBO', 'TBL_CHARGES_FARECLASS_XREF') }}   WHERE "_FIVETRAN_DELETED" = FALSE) AS TBL_CHARGES_FARECLASS_XREF ON TBL_GL_CHARGES_MAX.LNG_GL_CHARGES_ID_NMBR = TBL_CHARGES_FARECLASS_XREF.LNG_GL_CHARGES_ID_NMBR 
LEFT JOIN (SELECT * FROM {{ source('PSS_AMELIARES_DBO', 'TBL_FARE_CLASS') }}  tbl_Fare_Class WHERE "_FIVETRAN_DELETED" = FALSE) AS tbl_Fare_Class on TBL_CHARGES_FARECLASS_XREF.lng_Fare_Class_Id_Nmbr = tbl_Fare_Class.lng_Fare_Class_Id_Nmbr  
)

, tbl_Hold_Sched as (
			SELECT 
            str_Tail_Number_Identifier AS strACTail
            , str_Flight_Nmbr AS STR_FLIGHT_NMBR
            , dtm_Local_Dep_Date AS DTM_LOCAL_DEP_DATE
            , CAST(dtm_Dep_UTC_Date AS DATE) DTM_DEP_UTC_DATE
            , SD.str_Dep_Ident AS DEPARTURE_AIRPORT
            , SD.str_Arr_Ident AS ARRIVAL_AIRPORT
            , IFNULL(lng_Seat_Allocation_Id_Nmbr,-1) lng_Seat_Allocation_Id_Nmbr
            ,SD.str_Flight_Status
            , CAST(( SDLOS.lng_LOS_Seats + SDLOS.lng_Oversell_Seats ) - SDLOS.lng_LOS_CURTAIL AS FLOAT) AS Capacity
        FROM (SELECT * FROM {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_FLIGHT_WATCH_JOURNEY_LOG') }}  WHERE "_FIVETRAN_DELETED" = FALSE) AS SD
        LEFT JOIN (SELECT * FROM {{ source('PSS_AMELIARES_DBO', 'TBL_SKEDDETAIL_LEVELOFSERVICE') }}   WHERE "_FIVETRAN_DELETED" = FALSE)  AS SDLOS ON( SD.lng_Sked_Detail_Id_Nmbr = SDLOS.lng_Sked_Detail_Id_Nmbr 
        AND SDLOS.lng_LOS_Seats + SDLOS.lng_Oversell_Seats - SDLOS.lng_LOS_Curtail > 0 )
        
        LEFT JOIN (SELECT * FROM {{ source('PSS_AMELIARES_DBO', 'TBL_LEVEL_OF_SERVICE') }} tbl_Level_of_Service WHERE "_FIVETRAN_DELETED" = FALSE) AS LOS ON ( SDLOS.lng_Level_of_Service_Id_Nmbr = LOS.lng_Level_of_Service_Id_Nmbr)
        LEFT JOIN (SELECT * FROM {{ source('PSS_AMELIARES_DBO', 'TBL_SEAT_ALLOCATION') }}  tbl_Seat_Allocation WHERE "_FIVETRAN_DELETED" = FALSE) AS SA ON ( SDLOS.lng_SkedDetail_LevelofService_Id_Nmbr = SA.lng_SkedDetail_LevelofService_Id_Nmbr)
        WHERE SD.str_Flight_Status NOT IN ('N','X')
        )


,SUMMARY_DATA as ( SELECT strACTail as TAIL
                    ,t5.LNG_SKED_DETAIL_ID_NMBR
                    ,t1.STR_FLIGHT_NMBR
                    , t2.lng_Res_Legs_Id_Nmbr
                    , t3.lng_reservation_nmbr
                    ,DTM_LOCAL_DEP_DATE
                    ,DEPARTURE_AIRPORT
                    ,ARRIVAL_AIRPORT
                    ,Capacity
                    ,t3.str_GL_Charges_Desc
                   ,CASE
                       WHEN str_Res_CheckIn_Boarded = 'X'
                        THEN 1
                    ELSE 0
                    END NOSHOW
                   ,CASE
                       WHEN str_Res_CheckIn_Boarded = 'B'
                        THEN 1
                    ELSE 0
                    END Boarded
     --                ,CASE 
     --                    WHEN ((UPPER(t3.str_GL_Charges_Desc)  LIKE '%YID%' 
     --                    OR UPPER(t3.str_GL_Charges_Desc)  LIKE '%NRPS%' )
					-- OR (t3.lng_Fare_Category_Id_Nmbr = 16))
     --                    THEN 1
     --                    ELSE 0
                 ,CASE WHEN t3.lng_reservation_nmbr is null
                         THEN 1
                         ELSE 0
                        END NonRevenue
               FROM tbl_Hold_Sched t1
               LEFT JOIN  {{ source('PSS_AMELIARES_DBO', 'TBL_RES_SEGMENTS') }}  t2  ON  t1.lng_Seat_Allocation_Id_Nmbr = t2.lng_Seat_Allocation_Id_Nmbr and t2."_FIVETRAN_DELETED" = FALSE
                LEFT JOIN T_CHARGES_FARECLASS t3 ON  t3.lng_Res_Legs_Id_Nmbr = t2.lng_Res_Legs_Id_Nmbr 
                LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_RES_LEGS') }} t4 ON  t3.lng_Res_Legs_Id_Nmbr = t4.lng_Res_Legs_Id_Nmbr and t4."_FIVETRAN_DELETED" = FALSE
                LEFT JOIN {{ source('PSS_AMELIARES_DBO', 'TBL_SKED_DETAIL') }} t5 on t5.LNG_SKED_DETAIL_ID_NMBR = t2.LNG_SKED_DETAIL_ID_NMBR

               where  boarded = 1)

          
select LNG_SKED_DETAIL_ID_NMBR as "Flight_ID"
        ,DTM_LOCAL_DEP_DATE AS "FlightDate"
        ,STR_FLIGHT_NMBR as "FlightNum"
        ,DTM_LOCAL_DEP_DATE as "Departure"
        ,ARRIVAL_AIRPORT as "Arrival"
        ,TAIL as "Tail_Identifier"
        ,sum(Boarded) as "Total_PAX_OnBoard"
        ,(sum(Boarded) - sum(NonRevenue)) as "Total_PAX_Enplaned"
        ,sum(NonRevenue) as "Total_PAX_NonRev"
        ,0 "Total_PAX_FlowThru"
        ,0 "Total_PAX_Connecting"
        ,sum(Boarded) as "Total_PAX_Deplaned"
        --use no show for now
        ,sum(NOSHOW) as "Total_PAX_Deplaned2"
        ,Capacity
FROM SUMMARY_DATA
group by 1,2,3,4,5,6,14