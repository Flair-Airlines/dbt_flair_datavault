{% snapshot sked_details_snapshot %}

    {{
        config(
          target_database='DATAVAULT_DEV',
          target_schema='DV_AMELIA',
          strategy='timestamp',
          unique_key='LNG_SKED_DETAIL_ID_NMBR',
          updated_at='_FIVETRAN_SYNCED',
          invalidate_hard_deletes=True,
        )
    }}

    select * from "FIVETRAN_DATABASE"."PSS_AMELIARES_DBO"."SKED_DETAIL_DUMMYTABLE"

{% endsnapshot %}