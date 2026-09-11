-- stg_soap_orders — legacy SOAP OrderManagement orders (ADR-001/006/007).
-- Payload timestamps are naive UTC 'YYYY-MM-DD HH:MM:SS' strings → interpreted
-- as UTC and typed timestamptz; money arrives as Decimal(2dp) strings → numeric.
-- Status vocabulary is shared with stg_orders (tested).
{{ config(materialized='table') }}

select
    order_id                                              as order_id,
    (payload->>'customer_id')::bigint                     as customer_id,
    payload->>'status'                                    as status,
    (payload->>'total_amount')::numeric(12,2)             as total_amount,
    payload->>'currency'                                  as currency,
    (payload->>'created_at')::timestamp at time zone 'UTC' as created_at,
    (payload->>'updated_at')::timestamp at time zone 'UTC' as updated_at,
    batch_ref                                             as _batch_ref
from {{ source('helios_soap', 'soap_orders') }}
