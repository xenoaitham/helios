-- stg_file_customers — file-drop customers feed (ADR-004/006/007). The batch
-- PII surface: email + full_name pseudonymized here exactly like stg_users
-- (same salt, same macro → cross-feed hash equality, ADR-008 D1).
{{ config(materialized='table') }}

select
    customer_id                              as customer_id,
    {{ pii_hash("payload->>'email'") }}      as email_hash,
    {{ pii_hash("payload->>'full_name'") }}  as full_name_hash,
    payload->>'country_code'                 as country_code,
    (payload->>'signup_date')::date          as signup_date,
    payload->>'tier'                         as tier,
    batch_ref                                as _batch_ref
from {{ source('helios_file', 'file_customers') }}
