{% macro MIS_MISSING_LOADS() %}
  {% do run_query("call STAGE_PROD.MIS_DBO.RUN_QUERY_AND_EMAIL_IT();") %}
{% endmacro %}