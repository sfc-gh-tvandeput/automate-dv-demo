-- Validate order count consistency from staging through to facts
-- This ensures no order records are lost or duplicated through transformations

with staging_orders as (
    select count(distinct ORDER_PK) as cnt from {{ ref('v_stg_orders') }}
),
hub_orders as (
    select count(*) as cnt from {{ ref('hub_order') }}
),
fct_orders as (
    select count(*) as cnt from {{ ref('fct_orders') }}
)
select
    'staging' as layer,
    s.cnt as order_count,
    h.cnt as hub_count,
    f.cnt as fct_count
from staging_orders s, hub_orders h, fct_orders f
where s.cnt != h.cnt or h.cnt != f.cnt
