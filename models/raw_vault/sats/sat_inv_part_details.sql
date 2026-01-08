{%- set yaml_metadata -%}
source_model: v_stg_inventory
src_pk: PART_PK
src_hashdiff: PART_HASHDIFF
src_payload:
  - PART_NAME
  - PART_MFGR
  - PART_BRAND
  - PART_TYPE
  - PART_SIZE
  - PART_CONTAINER
  - PART_RETAILPRICE
  - PART_COMMENT
src_eff: EFFECTIVE_FROM
src_ldts: LOAD_DATE
src_source: RECORD_SOURCE
{%- endset -%}

{% set metadata = fromyaml(yaml_metadata) %}

{{ automate_dv.sat(src_pk=metadata['src_pk'],
                   src_hashdiff=metadata['src_hashdiff'],
                   src_payload=metadata['src_payload'],
                   src_eff=metadata['src_eff'],
                   src_ldts=metadata['src_ldts'],
                   src_source=metadata['src_source'],
                   source_model=metadata['source_model']) }}
