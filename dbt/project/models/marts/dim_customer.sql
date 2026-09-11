-- dim_customer — SCD2 customer dimension (ADR-009 D1/D2): a projection of
-- the protected snapshot, rebuilt every build (invariants re-derived from
-- state each run; the snapshot is the project's only persistent node).
-- Half-open validity window [valid_from, valid_to); valid_to IS NULL on the
-- current version. customer_sk = dbt_scd_id — deterministic, so facts can
-- resolve it by point-in-time window (fct_orders implements the join).
{{ config(materialized='table', schema='marts') }}

select
    dbt_scd_id                                as customer_sk,
    user_id                                   as customer_id,
    email_hash,
    full_name_hash,
    country_code,
    is_active,
    dbt_valid_from                            as valid_from,
    dbt_valid_to                              as valid_to,
    (dbt_valid_to is null)                    as is_current
from {{ ref('customers_snapshot') }}
