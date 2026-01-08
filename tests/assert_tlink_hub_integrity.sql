-- Validate t_link_transactions has valid hub references
-- This ensures transactional link integrity

select
    tl.TRANSACTION_PK
from {{ ref('t_link_transactions') }} tl
left join {{ ref('hub_customer') }} hc on tl.CUSTOMER_PK = hc.CUSTOMER_PK
left join {{ ref('hub_order') }} ho on tl.ORDER_PK = ho.ORDER_PK
where hc.CUSTOMER_PK is null or ho.ORDER_PK is null
