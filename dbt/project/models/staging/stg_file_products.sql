-- stg_file_products — file-drop products feed. Prices stay integer CENTS
-- (as-landed unit, docs/DATA_DICTIONARY.md); SKU lives in the shared
-- SKU-##### namespace (no identifiers in this feed — nothing to hash).
{{ config(materialized='table') }}

select
    sku                          as sku,
    payload->>'name'             as name,
    payload->>'category'         as category,
    (payload->>'price_cents')::integer as price_cents,
    payload->>'supplier_code'    as supplier_code,
    batch_ref                    as _batch_ref
from {{ source('helios_file', 'file_products') }}
