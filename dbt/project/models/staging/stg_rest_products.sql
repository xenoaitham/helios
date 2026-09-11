-- stg_rest_products — REST Pricing API catalog walk (ADR-003/006/007).
-- Money crosses the API as integer cents and stays cents; same price formula
-- and SKU namespace as the other product feeds (coherent joins by design).
{{ config(materialized='table') }}

select
    id                           as product_id,
    payload->>'sku'              as sku,
    payload->>'name'             as name,
    payload->>'category'         as category,
    payload->>'currency'         as currency,
    (payload->>'price_cents')::integer as price_cents,
    batch_ref                    as _batch_ref
from {{ source('helios_rest', 'rest_products') }}
