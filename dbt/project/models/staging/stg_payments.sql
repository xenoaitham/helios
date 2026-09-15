-- stg_payments — current OLTP payments, one row per payment_id. REFUNDED rows
-- carry the negated amount by source contract (docs/DATA_DICTIONARY.md).
{{ config(materialized='table') }}

select
    (after->>'payment_id')::bigint          as payment_id,
    (after->>'order_id')::bigint            as order_id,
    after->>'method'                        as method,
    (after->>'amount')::numeric(12,2)       as amount,
    after->>'status'                        as status,
    (after->>'paid_at')::timestamptz        as paid_at,
    lsn                                     as _cdc_lsn
from {{ source('helios_oltp', 'cdc_payments') }}
where op <> 'd'
