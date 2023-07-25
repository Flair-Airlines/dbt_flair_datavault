{% macro run_mis_scd_proc() %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_PAX();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_PAX_ANCILLARY_CHARGES();") %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_FLIGHT_SEGMENTS_TO_BAGS();") %}
{% endmacro %}