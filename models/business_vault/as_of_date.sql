{{- config(materialized='table') -}}

{%- set yaml_metadata -%}
datepart: day
start_date: "TO_DATE('1992-01-01')"
end_date: "TO_DATE('1998-12-31')"
{%- endset -%}

{% set metadata = fromyaml(yaml_metadata) %}

WITH as_of_date AS (
    {{ dbt_utils.date_spine(
        datepart=metadata['datepart'],
        start_date=metadata['start_date'],
        end_date=metadata['end_date']
    ) }}
)

SELECT DATE_DAY AS AS_OF_DATE
FROM as_of_date
