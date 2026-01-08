{{- config(
    materialized='incremental',
    unique_key='ORDER_SK',
    incremental_strategy='merge'
) -}}

with orders_ranked as (
    select
        o.ORDER_PK,
        o.ORDERSTATUS,
        o.TOTALPRICE,
        o.ORDERDATE,
        o.ORDERPRIORITY,
        o.CLERK,
        o.SHIPPRIORITY,
        o.ORDER_COMMENT,
        o.LOAD_DATE,
        row_number() over (partition by o.ORDER_PK order by o.LOAD_DATE desc) as rn
    from {{ ref('sat_order_order_details') }} o
    {% if is_incremental() %}
        where o.LOAD_DATE > (select max(LOAD_DATE) from {{ this }})
    {% endif %}
),

orders as (
    select * from orders_ranked where rn = 1
),

customer_orders as (
    select
        lco.ORDER_CUSTOMER_PK,
        lco.CUSTOMER_PK,
        lco.ORDER_PK
    from {{ ref('link_customer_order') }} lco
),

dim_customer_current as (
    select
        CUSTOMER_SK,
        CUSTOMER_PK
    from {{ ref('dim_customer') }}
    where IS_CURRENT = true
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['o.ORDER_PK']) }} as ORDER_SK,
        o.ORDER_PK,
        coalesce(dc.CUSTOMER_SK, 'UNKNOWN') as CUSTOMER_SK,
        co.CUSTOMER_PK,
        o.ORDERDATE as ORDER_DATE,
        o.ORDERSTATUS as ORDER_STATUS,
        o.ORDERPRIORITY as ORDER_PRIORITY,
        o.TOTALPRICE as TOTAL_PRICE,
        o.CLERK,
        o.SHIPPRIORITY as SHIP_PRIORITY,
        o.ORDER_COMMENT,
        o.LOAD_DATE,
        current_timestamp() as DBT_UPDATED_AT
    from orders o
    inner join customer_orders co
        on o.ORDER_PK = co.ORDER_PK
    left join dim_customer_current dc
        on co.CUSTOMER_PK = dc.CUSTOMER_PK
)

select * from final