{% macro mis_scd_proc_airflow_test() %}
  {% do run_query("call STAGE_DEV.MIS_DBO.PROC_STG_PAX2();") %}
 
{% endmacro %}