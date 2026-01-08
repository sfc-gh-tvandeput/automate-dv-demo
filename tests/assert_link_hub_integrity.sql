-- Validate link records have valid hub references on both ends
-- This ensures link integrity between hubs

select
    link.ORDER_CUSTOMER_PK
from {{ ref('link_customer_order') }} link
left join {{ ref('hub_customer') }} hc on link.CUSTOMER_PK = hc.CUSTOMER_PK
left join {{ ref('hub_order') }} ho on link.ORDER_PK = ho.ORDER_PK
where hc.CUSTOMER_PK is null or ho.ORDER_PK is null
