-- Validate all hub_customer records have at least one satellite record
-- This ensures satellites are populated for all business keys

select
    hub.CUSTOMER_PK
from {{ ref('hub_customer') }} hub
left join {{ ref('sat_order_customer_details') }} sat
    on hub.CUSTOMER_PK = sat.CUSTOMER_PK
where sat.CUSTOMER_PK is null
