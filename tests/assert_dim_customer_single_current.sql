-- Validate dim_customer SCD2 has exactly one current record per customer
-- This ensures proper SCD2 implementation

select
    CUSTOMER_PK,
    count(*) as current_count
from {{ ref('dim_customer') }}
where IS_CURRENT = true
group by CUSTOMER_PK
having count(*) != 1
