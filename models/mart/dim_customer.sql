{{- config(
    materialized='incremental',
    unique_key='CUSTOMER_SK',
    incremental_strategy='merge',
    merge_update_columns=['VALID_TO', 'IS_CURRENT', 'DBT_UPDATED_AT']
) -}}

with pit_records as (
    select
        CUSTOMER_PK,
        AS_OF_DATE,
        SAT_ORDER_CUSTOMER_DETAILS_PK,
        SAT_ORDER_CUSTOMER_DETAILS_LDTS
    from {{ ref('pit_customer') }}
    where SAT_ORDER_CUSTOMER_DETAILS_LDTS != '1900-01-01 00:00:00'
    {% if is_incremental() %}
        and AS_OF_DATE > (select max(VALID_FROM) from {{ this }})
    {% endif %}
),

sat_deduped as (
    select *
    from {{ ref('sat_order_customer_details') }}
    qualify row_number() over (partition by CUSTOMER_PK, LOAD_DATE order by LOAD_DATE) = 1
),

customer_payload as (
    select
        p.CUSTOMER_PK,
        p.AS_OF_DATE,
        s.CUSTOMER_NAME,
        s.CUSTOMER_ADDRESS,
        s.CUSTOMER_PHONE,
        s.CUSTOMER_ACCBAL,
        s.CUSTOMER_MKTSEGMENT,
        s.CUSTOMER_COMMENT,
        s.CUSTOMER_HASHDIFF
    from pit_records p
    inner join sat_deduped s
        on p.CUSTOMER_PK = s.CUSTOMER_PK
        and p.SAT_ORDER_CUSTOMER_DETAILS_LDTS = s.LOAD_DATE
),

{% if is_incremental() %}
existing_current as (
    select
        e.CUSTOMER_SK,
        e.CUSTOMER_PK,
        e.CUSTOMER_NAME,
        e.CUSTOMER_ADDRESS,
        e.CUSTOMER_PHONE,
        e.CUSTOMER_ACCBAL,
        e.CUSTOMER_MKTSEGMENT,
        e.CUSTOMER_COMMENT,
        e.VALID_FROM,
        s.CUSTOMER_HASHDIFF
    from {{ this }} e
    inner join {{ ref('sat_order_customer_details') }} s
        on e.CUSTOMER_PK = s.CUSTOMER_PK
        and e.VALID_FROM = s.LOAD_DATE
    where e.IS_CURRENT = true
),

change_detection as (
    select
        cp.CUSTOMER_PK,
        cp.AS_OF_DATE,
        cp.CUSTOMER_NAME,
        cp.CUSTOMER_ADDRESS,
        cp.CUSTOMER_PHONE,
        cp.CUSTOMER_ACCBAL,
        cp.CUSTOMER_MKTSEGMENT,
        cp.CUSTOMER_COMMENT,
        cp.CUSTOMER_HASHDIFF,
        coalesce(
            lag(cp.CUSTOMER_HASHDIFF) over (
                partition by cp.CUSTOMER_PK 
                order by cp.AS_OF_DATE
            ),
            ec.CUSTOMER_HASHDIFF
        ) as PREV_HASHDIFF,
        ec.VALID_FROM as EXISTING_VALID_FROM
    from customer_payload cp
    left join existing_current ec
        on cp.CUSTOMER_PK = ec.CUSTOMER_PK
),
{% else %}
change_detection as (
    select
        CUSTOMER_PK,
        AS_OF_DATE,
        CUSTOMER_NAME,
        CUSTOMER_ADDRESS,
        CUSTOMER_PHONE,
        CUSTOMER_ACCBAL,
        CUSTOMER_MKTSEGMENT,
        CUSTOMER_COMMENT,
        CUSTOMER_HASHDIFF,
        lag(CUSTOMER_HASHDIFF) over (
            partition by CUSTOMER_PK 
            order by AS_OF_DATE
        ) as PREV_HASHDIFF,
        cast(null as timestamp) as EXISTING_VALID_FROM
    from customer_payload
),
{% endif %}

change_points as (
    select
        CUSTOMER_PK,
        AS_OF_DATE as VALID_FROM,
        CUSTOMER_NAME,
        CUSTOMER_ADDRESS,
        CUSTOMER_PHONE,
        CUSTOMER_ACCBAL,
        CUSTOMER_MKTSEGMENT,
        CUSTOMER_COMMENT,
        CUSTOMER_HASHDIFF,
        EXISTING_VALID_FROM
    from change_detection
    where PREV_HASHDIFF is null 
       or CUSTOMER_HASHDIFF != PREV_HASHDIFF
),

validity_ranges as (
    select
        CUSTOMER_PK,
        VALID_FROM,
        coalesce(
            lead(VALID_FROM) over (
                partition by CUSTOMER_PK 
                order by VALID_FROM
            ),
            to_timestamp('9999-12-31 23:59:59')
        ) as VALID_TO,
        CUSTOMER_NAME,
        CUSTOMER_ADDRESS,
        CUSTOMER_PHONE,
        CUSTOMER_ACCBAL,
        CUSTOMER_MKTSEGMENT,
        CUSTOMER_COMMENT,
        EXISTING_VALID_FROM
    from change_points
),

new_records as (
    select
        {{ dbt_utils.generate_surrogate_key(['CUSTOMER_PK', 'VALID_FROM']) }} as CUSTOMER_SK,
        CUSTOMER_PK,
        CUSTOMER_NAME,
        CUSTOMER_ADDRESS,
        CUSTOMER_PHONE,
        CUSTOMER_ACCBAL,
        CUSTOMER_MKTSEGMENT,
        CUSTOMER_COMMENT,
        VALID_FROM,
        VALID_TO,
        case when VALID_TO = to_timestamp('9999-12-31 23:59:59') then true else false end as IS_CURRENT,
        current_timestamp() as DBT_UPDATED_AT
    from validity_ranges
)

{% if is_incremental() %}
,close_existing as (
    select
        {{ dbt_utils.generate_surrogate_key(['vr.CUSTOMER_PK', 'vr.EXISTING_VALID_FROM']) }} as CUSTOMER_SK,
        vr.CUSTOMER_PK,
        e.CUSTOMER_NAME,
        e.CUSTOMER_ADDRESS,
        e.CUSTOMER_PHONE,
        e.CUSTOMER_ACCBAL,
        e.CUSTOMER_MKTSEGMENT,
        e.CUSTOMER_COMMENT,
        vr.EXISTING_VALID_FROM as VALID_FROM,
        vr.VALID_FROM as VALID_TO,
        false as IS_CURRENT,
        current_timestamp() as DBT_UPDATED_AT
    from validity_ranges vr
    inner join {{ this }} e
        on vr.CUSTOMER_PK = e.CUSTOMER_PK
        and vr.EXISTING_VALID_FROM = e.VALID_FROM
    where vr.EXISTING_VALID_FROM is not null
      and e.IS_CURRENT = true
)

select * from new_records
where CUSTOMER_SK not in (select CUSTOMER_SK from close_existing)
union all
select * from close_existing
{% else %}
select * from new_records
{% endif %}