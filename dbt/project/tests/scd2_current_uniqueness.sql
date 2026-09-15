-- SCD2 invariant: exactly one is_current version per customer (ADR-009 D6).
-- Any row returned is a customer with 0 or ≥2 current versions. (Customers
-- with zero current rows at all are caught by the dim_customer parity test —
-- current customers must equal stg_users exactly.)
select
    customer_id,
    count(*) as current_versions
from {{ ref('dim_customer') }}
where is_current
group by customer_id
having count(*) <> 1
