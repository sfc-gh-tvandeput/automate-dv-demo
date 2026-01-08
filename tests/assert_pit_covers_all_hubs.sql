-- Validate PIT table covers all hub customers
-- This ensures business vault has timeline for every customer

select
    hub.CUSTOMER_PK
from {{ ref('hub_customer') }} hub
left join {{ ref('pit_customer') }} pit
    on hub.CUSTOMER_PK = pit.CUSTOMER_PK
where pit.CUSTOMER_PK is null
