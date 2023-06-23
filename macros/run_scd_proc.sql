{% macro run_scd_proc() %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DV_D_SKED_DETAILS();") %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DV_D_RES_LEGS();") %}
  {% do run_query("call DATAVAULT_DEV.DV_AMELIA.PROC_DV_D_RES_SEGMENTS();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_AGENCY_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_AIRPORT_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_CHARGE_CATEGORY_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_COUNTRY_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_CURRENCY_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_GL_CHARGE_TYPE_DEFINITION_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_PROVINCE_DEFINITION_SCD();") %}
  --{% do run_query("call STAGE_DEV.AMELIA.PROC_STG_RES_HEADER_SCD();") %}
  --{% do run_query("call STAGE_DEV.AMELIA.PROC_STG_RES_LEGS_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_SKED_DETAIL_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_SKED_HEADER_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_SKED_MASTER_DETAIL_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_SKED_MASTER_HEADER_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_TAX_CONFIGURATION_SCD();") %}
  {% do run_query("call STAGE_DEV.AMELIA.PROC_STG_USERS_SCD();") %}
  --{% do run_query("call STAGE_DEV.AMELIA.PROC_STG_PAX_SCD();") %}
  --{% do run_query("call STAGE_DEV.AMELIA.PROC_STG_RES_SEGMENTS_SCD();") %}
{% endmacro %}