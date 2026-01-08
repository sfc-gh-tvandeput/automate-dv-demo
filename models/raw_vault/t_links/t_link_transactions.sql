{%- set yaml_metadata -%}
source_model: v_stg_transactions
src_pk: TRANSACTION_PK
src_fk:
  - CUSTOMER_PK
  - ORDER_PK
src_payload:
  - TRANSACTION_NUMBER
  - TRANSACTION_DATE
  - TYPE
  - AMOUNT
src_eff: EFFECTIVE_FROM
src_ldts: LOAD_DATE
src_source: RECORD_SOURCE
{%- endset -%}

{% set metadata = fromyaml(yaml_metadata) %}

{{ automate_dv.t_link(src_pk=metadata['src_pk'],
                      src_fk=metadata['src_fk'],
                      src_payload=metadata['src_payload'],
                      src_eff=metadata['src_eff'],
                      src_ldts=metadata['src_ldts'],
                      src_source=metadata['src_source'],
                      source_model=metadata['source_model']) }}
