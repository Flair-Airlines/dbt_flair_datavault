{% macro run_scd_proc() %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DV_D_SKED_DETAILS();") %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DV_D_RES_LEGS();") %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DV_D_RES_SEGMENTS();") %}
{% endmacro %}