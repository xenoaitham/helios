-- stg_rest_promotions — REST promotions walk. product_sku must exist in the
-- shared catalog (rest ∪ file products) — tested total, ADR-008 D6.
{{ config(materialized='table') }}

select
    id                                    as promotion_id,
    payload->>'product_sku'               as product_sku,
    (payload->>'discount_percent')::integer as discount_percent,
    (payload->>'starts_at')::date         as starts_at,
    (payload->>'ends_at')::date           as ends_at,
    payload->>'description'               as description,
    batch_ref                             as _batch_ref
from {{ source('helios_rest', 'rest_promotions') }}
