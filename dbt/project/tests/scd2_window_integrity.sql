-- SCD2 invariant: window integrity (ADR-009 D2/D6).
--   1. a closed window must end after it starts (valid_to > valid_from);
--   2. chains are contiguous half-open intervals: a version's valid_to must
--      equal the next version's valid_from (no gaps, no overlaps);
--   3. only the last version in a chain may have valid_to IS NULL.
with versions as (
    select
        customer_id,
        customer_sk,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by customer_id order by valid_from
        ) as next_valid_from
    from {{ ref('dim_customer') }}
)
select
    customer_id,
    customer_sk,
    valid_from,
    valid_to,
    next_valid_from
from versions
where (valid_to is not null and valid_to <= valid_from)
   or (valid_to is not null and next_valid_from is not null
       and valid_to <> next_valid_from)
   or (valid_to is null and next_valid_from is not null)
