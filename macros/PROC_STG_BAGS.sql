{% macro PROC_STG_BAGS() %}
  {% do run_query("CREATE OR REPLACE PROCEDURE STAGE_DEV.MIS_DBO.PROC_STG_BAGS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
  /*###############################################################################################################################################

    #    Date                Name                 Version                    Comment         #

    #    08-08-2023          NIVETHA             Initial Version             Loading RAW-STG BAGS with TYPE2 SCD and no transformations  #

    #################################################################################################################################################*/
BEGIN
MERGE	
INTO
	STAGE_DEV.MIS_DBO.STG_BAGS TGT
USING
(
--Getting New records to insert into Target		
SELECT
NULL AS ID_KEY,
RAW_BAGS.*
FROM
FIVETRAN_DATABASE.MIS_DBO.BAGS RAW_BAGS
	
UNION ALL
 	--Getting existing records to update the end date and active flag
(
	SELECT
		STG_BAGS.ID AS ID_KEY,
		STG_BAGS.*
		EXCLUDE (EFFECTIVE_START_DATE,
		EFFECTIVE_END_DATE,
		ACTIVE_FLAG,
		TGT_CREATE_DT,
		TGT_UPDATE_DT
		)
	FROM
		STAGE_DEV.MIS_DBO.STG_BAGS STG_BAGS
	JOIN 
		FIVETRAN_DATABASE.MIS_DBO.BAGS RAW_BAGS
	ON
		STG_BAGS.ID = RAW_BAGS.ID
		AND STG_BAGS.EFFECTIVE_END_DATE > CURRENT_TIMESTAMP()
		
	) 
) SRC 
ON
	(TGT.ID = SRC.ID_KEY
		AND TGT.EFFECTIVE_END_DATE > CURRENT_TIMESTAMP())
	
	WHEN MATCHED THEN
UPDATE
SET
	TGT.EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(),
	TGT.ACTIVE_FLAG = ''N''	,
	TGT.TGT_UPDATE_DT = CURRENT_TIMESTAMP()	
	
	WHEN NOT MATCHED THEN
INSERT
(
ID,
OwnerType,
OwnerID,
BagTypeID,
PieceNo,
BagTagPrefix,
BagNo,
IsManualBagNo,
Weight,
IsHeavy,
IsOversized,
IsExcess,
ExcessWeight,
IsPrePaid,
IsGateCheck,
IsPriority,
LastName,
FirstName,
Remarks,
PrintCount,
IsActivated,
IsBsmRequired,
IsBsmSent,
OverrideStatus,
IsDamaged,
IsDeleted,
CustomData,
CreatedLocation,
CreatedBy,
CreatedTimestampUTC,
UpdatedBy,
UpdatedTimestampUTC,
IssuedByCarrierCode,
IssuedByAirportCode,
OntoFlightNo1,
OntoFlightDate1,
OntoFlightOrigin1,
OntoFlightDestination1,
OntoFlightNo2,
OntoFlightDate2,
OntoFlightOrigin2,
OntoFlightDestination2,
IntoConnectingBagID,
PrintedTimestampUTC,
SbdInductedTimestampUTC,
SortedTimestampUTC,
LoadedTimestampUTC,
OffloadedTimestampUTC,
UnloadedTimestampUTC,
DeliveredTimestampUTC,
_cdatasync_deleted,
TGT_CREATE_DT,
TGT_UPDATE_DT,
_FIVETRAN_DELETED,
_FIVETRAN_SYNCED,
EFFECTIVE_START_DATE ,
EFFECTIVE_END_DATE,
ACTIVE_FLAG 
  )

  
VALUES 
  (
SRC.ID,
SRC.OwnerType,
SRC.OwnerID,
SRC.BagTypeID,
SRC.PieceNo,
SRC.BagTagPrefix,
SRC.BagNo,
SRC.IsManualBagNo,
SRC.Weight,
SRC.IsHeavy,
SRC.IsOversized,
SRC.IsExcess,
SRC.ExcessWeight,
SRC.IsPrePaid,
SRC.IsGateCheck,
SRC.IsPriority,
SRC.LastName,
SRC.FirstName,
SRC.Remarks,
SRC.PrintCount,
SRC.IsActivated,
SRC.IsBsmRequired,
SRC.IsBsmSent,
SRC.OverrideStatus,
SRC.IsDamaged,
SRC.IsDeleted,
SRC.CustomData,
SRC.CreatedLocation,
SRC.CreatedBy,
SRC.CreatedTimestampUTC,
SRC.UpdatedBy,
SRC.UpdatedTimestampUTC,
SRC.IssuedByCarrierCode,
SRC.IssuedByAirportCode,
SRC.OntoFlightNo1,
SRC.OntoFlightDate1,
SRC.OntoFlightOrigin1,
SRC.OntoFlightDestination1,
SRC.OntoFlightNo2,
SRC.OntoFlightDate2,
SRC.OntoFlightOrigin2,
SRC.OntoFlightDestination2,
SRC.IntoConnectingBagID,
SRC.PrintedTimestampUTC,
SRC.SbdInductedTimestampUTC,
SRC.SortedTimestampUTC,
SRC.LoadedTimestampUTC,
SRC.OffloadedTimestampUTC,
SRC.UnloadedTimestampUTC,
SRC.DeliveredTimestampUTC,
SRC._cdatasync_deleted,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP(),
SRC._FIVETRAN_DELETED,
SRC._FIVETRAN_SYNCED,
CURRENT_TIMESTAMP(),
CAST(''9999-12-31 23:59:59'' AS TIMESTAMP),
''Y''
);

update STAGE_DEV.MIS_DBO.STG_BAGS
set
EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(),
ACTIVE_FLAG = ''N''	,
TGT_UPDATE_DT = CURRENT_TIMESTAMP()	
where
_FIVETRAN_DELETED = ''TRUE''
and  EFFECTIVE_END_DATE > CURRENT_TIMESTAMP()
;

TRUNCATE TABLE FIVETRAN_DATABASE.MIS_DBO.BAGS;

END;

';") %}
{% endmacro %}