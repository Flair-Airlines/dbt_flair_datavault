{% macro run_mis_scd_proc() %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_PAX();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_PAX_ANCILLARY_CHARGES();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_FLIGHT_SEGMENTS_TO_BAGS();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_PASSENGER_ANCILLARY_CHARGE_TAXES();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_PAX_ANCILLARY_PAYMENTS();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_AIRPORTS();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_ANCILLARY_BAG_CHARGES();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_ANCILLARY_CHARGE_CATEGORIES();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_ANCILLARY_MISCELLANEOUS_CHARGES();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_ANCILLARY_SEAT_CHARGES();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_ANCILLARY_TICKET_CHARGES();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_FLIGHT_SEGMENTS();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_PAX_ANCILLARY_TRANSACTIONS();") %}  
{% endmacro %}