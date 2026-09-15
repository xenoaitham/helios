-- stg_orders — current OLTP orders, one row per order_id. Tombstone deletes
-- are filtered here (ADR-005 contract); strings from the envelope are cast to
-- native types (raw stays verbatim).
{{ config(materialized='table') }}

select
    (after->>'order_id')::bigint            as order_id,
    (after->>'user_id')::bigint             as user_id,
    after->>'status'                        as status,
    after->>'currency'                      as currency,
    (after->>'total_amount')::numeric(12,2) as total_amount,
    (after->>'placed_at')::timestamptz      as placed_at,
    (after->>'updated_at')::timestamptz     as updated_at,
    lsn                                     as _cdc_lsn
from {{ source('helios_oltp', 'cdc_orders') }}
where op <> 'd'
