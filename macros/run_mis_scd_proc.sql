{% macro run_mis_scd_proc() %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_PAX();") %}
{% endmacro %}