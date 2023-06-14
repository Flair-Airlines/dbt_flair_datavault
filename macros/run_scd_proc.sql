{% macro run_scd_proc() %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DM_TBL_SKED_DETAILS_SCD();") %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DM_TBL_RES_LEGS_SCD();") %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DM_TBL_RES_SEGMENTS_SCD();") %}
{% endmacro %}