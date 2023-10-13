{% macro AMELIA_CDC_CHECK_STAGE_LOAD() %}
  {% do run_query("call FIVETRAN_DATABASE.MIS_TEST2_DBO.PROC_STG_GL_CREDITCARD_PAYMENT();") %}
{% endmacro %}