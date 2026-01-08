-- Validate all customers from staging exist in hub_customer
-- This ensures no data loss during staging → raw vault transformation

select
    stg.CUSTOMER_PK
from {{ ref('v_stg_orders') }} stg
left join {{ ref('hub_customer') }} hub
    on stg.CUSTOMER_PK = hub.CUSTOMER_PK
where hub.CUSTOMER_PK is null
group by stg.CUSTOMER_PK
