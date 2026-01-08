{{- config(materialized='pit_incremental') -}}

{%- set yaml_metadata -%}
source_model: hub_customer
src_pk: CUSTOMER_PK
src_ldts: LOAD_DATE
as_of_dates_table: as_of_date
satellites:
  sat_order_customer_details:
    pk:
      PK: CUSTOMER_PK
    ldts:
      LDTS: LOAD_DATE
stage_tables_ldts:
  v_stg_orders: LOAD_DATE
{%- endset -%}

{% set metadata = fromyaml(yaml_metadata) %}

{{ automate_dv.pit(src_pk=metadata['src_pk'],
                   src_extra_columns=none,
                   as_of_dates_table=metadata['as_of_dates_table'],
                   satellites=metadata['satellites'],
                   stage_tables_ldts=metadata['stage_tables_ldts'],
                   src_ldts=metadata['src_ldts'],
                   source_model=metadata['source_model']) }}
