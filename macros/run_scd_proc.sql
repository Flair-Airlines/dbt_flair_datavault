{% macro run_scd_proc() %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DM_TBL_SKED_DETAILS_SCD();") %}
{% endmacro %}