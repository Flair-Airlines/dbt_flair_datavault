{% snapshot sked_details_snap_cols %}

    {{
        config(
          target_database='DATAVAULT_DEV',
          target_schema='DV_AMELIA',
          strategy='check',
          unique_key='LNG_SKED_DETAIL_ID_NMBR',
          check_cols = ['LNG_AIRCRAFT_ID_NMBR','STR_AIRLINE_CODES_IDENT','STR_SKED_DETAIL_STATUS','STR_GATE_NMBR','LNG_LAST_MOD_USER_ID_NMBR','LNG_SKED_MASTER_DETAIL_ID_NMBR','STR_ALLOWTHRU','STR_SKED_DETAIL_TYPE','STR_FLIGHT_NMBR'],
          invalidate_hard_deletes=True,
        )
    }}

    select * from "FIVETRAN_DATABASE"."PSS_AMELIARES_DBO"."SKED_DETAIL_DUMMYTABLE"

{% endsnapshot %}