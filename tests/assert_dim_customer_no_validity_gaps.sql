-- Validate dim_customer SCD2 has no gaps in validity periods
-- This ensures continuous timeline for each customer

with validity_check as (
    select
        CUSTOMER_PK,
        VALID_TO,
        lead(VALID_FROM) over (partition by CUSTOMER_PK order by VALID_FROM) as NEXT_VALID_FROM
    from {{ ref('dim_customer') }}
)
select
    CUSTOMER_PK,
    VALID_TO,
    NEXT_VALID_FROM
from validity_check
where NEXT_VALID_FROM is not null
  and VALID_TO != NEXT_VALID_FROM
