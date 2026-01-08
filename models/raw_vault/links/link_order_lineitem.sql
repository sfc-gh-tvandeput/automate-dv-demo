{%- set yaml_metadata -%}
source_model: v_stg_orders
src_pk: LINK_LINEITEM_ORDER_PK
src_fk:
  - ORDER_PK
  - LINEITEM_PK
src_ldts: LOAD_DATE
src_source: RECORD_SOURCE
{%- endset -%}

{% set metadata = fromyaml(yaml_metadata) %}

{{ automate_dv.link(src_pk=metadata['src_pk'],
                    src_fk=metadata['src_fk'],
                    src_ldts=metadata['src_ldts'],
                    src_source=metadata['src_source'],
                    source_model=metadata['source_model']) }}
