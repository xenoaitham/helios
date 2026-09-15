-- stg_order_items — current OLTP order items, one row per order_item_id.
-- The largest staging model (~4.3M live rows): the full-refresh scan of the
-- 4.4M-row envelope is the build's long pole (ADR-008 D2, documented).
{{ config(materialized='table') }}

select
    (after->>'order_item_id')::bigint       as order_item_id,
    (after->>'order_id')::bigint            as order_id,
    after->>'product_sku'                   as product_sku,
    (after->>'quantity')::integer           as quantity,
    (after->>'unit_price')::numeric(10,2)   as unit_price,
    lsn                                     as _cdc_lsn
from {{ source('helios_oltp', 'cdc_order_items') }}
where op <> 'd'
