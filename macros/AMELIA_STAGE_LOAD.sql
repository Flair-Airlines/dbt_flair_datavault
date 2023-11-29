{% macro AMELIA_STAGE_LOAD() %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_CHARGES_FARECLASS_XREF();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_FAMILY_DEFINITION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_PAX();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_RES_LEGS();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_SEAT_ALLOCATION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FAREHEADER_AIRPORT_INCLUSION_XREF();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_CATEGORY();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_GL_CHARGES();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_RES_SEGMENTS();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_DETAIL();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_HEADER();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_VISIBILITY_TYPE();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_CLASS_SEAT_CHARGE_TYPE();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_CATEGORY_ASSOCIATED();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_CLASS_MARKUP();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FAREHEADER_GLSURCHARGE_CURRENCY();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_SKED_FLIGHT_WATCH_JOURNEY_LOG();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_CATEGORY_NESTING();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_CATEGORY_VISIBILITY_XREF();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_DETAIL_CURRENCY();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FAREHEADER_GLSURCHARGE_XREF();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_ALLOCATION_TEMPLATE();") %}    
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_CATEGORY_SEAT_CHARGE_CURRENCY();") %} 
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_CLASS();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_DETAIL_PROMO_CODE();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_LEVEL_OF_SERVICE();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_SKEDDETAIL_LEVELOFSERVICE();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_CLASS_VISIBILITY_XREF();") %}  
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_AGENCY();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_AIRCRAFT_DEFINITION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_AIRPORT();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_CHARGE_CATEGORY();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_COUNTRY();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_CURRENCY();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_DISTRIBUTION_CHANNEL();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FLIGHT_STATUS_DEFINITION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FLIGHT_TYPE_DEFINITION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_GL_CHARGE_TYPE_DEFINITION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_PAX_REQUESTS_DEFINITION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_RES_PAX_GROUP_REQUESTS_XREF();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_PROVINCE_DEFINITION();") %}    
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_RES_HEADER();") %} 
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_RES_PAX_GROUP();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_RES_STANDBY_SEGMENTS();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_SKED_DETAIL();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_TAX_CONFIGURATION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_USERS();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_FARE_ALLOCATION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_GL_PAYMENT_METHOD();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_GL_REFUND_CC_XREF();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_GL_PAYMENTS_REFUND();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_CC_TRACK();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_GL_CREDITCARD_PAYMENT();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_GL_PAYMENTS();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_GL_PAYMENT_CC_XREF();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_BULK_MOVE_TRACKING();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_GL_PAYMENT_ASSIGNMENT();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO. PROC_STG_RES_CHECKIN ();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_RESERVATIONS_ALERTS_TYPE_DEFINITION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_RESERVATIONS_ALERTS_CATEGORY();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_RESERVATIONS_ALERTS_TYPE_CATEGORY_XREF();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_RESERVATIONS_ALERTS_HEADER();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_AIRCRAFT_MODEL_DEFINITION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_GL_CHARGES_MOD_AUDIT();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_VOUCHERS();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_VOUCHER_DEFINITION();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_VOUCHER_PAYMENTS_XREF();") %}
  {% do run_query("call STAGE_PROD.AMELIA_DBO.PROC_STG_SSR_RES_LEG_BOOKING_LIFECYCLE_XREF();") %}
{% endmacro %}
