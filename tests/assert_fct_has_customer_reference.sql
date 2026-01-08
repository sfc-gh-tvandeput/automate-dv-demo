-- Validate fct_orders has valid dimension references
-- This ensures fact table correctly resolves all customer lookups (UNKNOWN is acceptable)

select
    fct.ORDER_PK
from {{ ref('fct_orders') }} fct
where fct.CUSTOMER_SK is null
