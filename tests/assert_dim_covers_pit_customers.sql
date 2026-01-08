-- Validate dim_customer covers all customers in PIT
-- This ensures mart layer includes all business vault customers

select
    pit.CUSTOMER_PK
from {{ ref('pit_customer') }} pit
left join {{ ref('dim_customer') }} dim
    on pit.CUSTOMER_PK = dim.CUSTOMER_PK
where dim.CUSTOMER_PK is null
  and pit.SAT_ORDER_CUSTOMER_DETAILS_LDTS != '1900-01-01 00:00:00'
group by pit.CUSTOMER_PK
