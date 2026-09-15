-- stg_users — current OLTP users, one row per user_id (ADR-008 D2/D3).
-- PII contract: email + full_name are pseudonymized HERE (macro pii_hash);
-- raw.cdc_users keeps cleartext; nothing downstream ever sees it.
{{ config(materialized='table') }}

select
    (after->>'user_id')::bigint            as user_id,
    {{ pii_hash("after->>'email'") }}      as email_hash,
    {{ pii_hash("after->>'full_name'") }}  as full_name_hash,
    after->>'country_code'                 as country_code,
    (after->>'is_active')::boolean         as is_active,
    (after->>'created_at')::timestamptz    as created_at,
    (after->>'last_login_at')::timestamptz as last_login_at,
    lsn                                    as _cdc_lsn
from {{ source('helios_oltp', 'cdc_users') }}
where op <> 'd'
