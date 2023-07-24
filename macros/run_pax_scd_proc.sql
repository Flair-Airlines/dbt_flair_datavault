{% macro run_scd_proc() %}
  {% do run_query("call STAGE_DEV.MIS.PROC_STG_PAX();") %}
{% endmacro %}