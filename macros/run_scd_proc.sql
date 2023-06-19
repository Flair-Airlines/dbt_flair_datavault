{% macro run_scd_proc() %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DV_D_SKED_DETAILS();") %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DV_D_RES_LEGS();") %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DV_D_RES_SEGMENTS();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_SKED_DETAIL_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_SKED_HEADER_SCD();") %}
{% endmacro %}