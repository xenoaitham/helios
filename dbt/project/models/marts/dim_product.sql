-- dim_product — SCD1 product dimension over the shared SKU-##### space:
-- stg_rest_products ∪ stg_file_products (ADR-009 D4). SCD1 is the decision:
-- prices are formula-stable and agree 105/105 on the measured overlap;
-- versioning would be ceremony. Natural key = sku (SCD1 → the natural key IS
-- the stable key; a surrogate would add a join and zero information).
-- Precedence on overlap: REST wins (fresher cadence, carries currency);
-- file-only attributes stay NULL on REST rows; source_feed is provenance.
{{ config(materialized='table', schema='marts') }}

with unioned as (
    select
        sku,
        name,
        category,
        price_cents,
        currency,
        null::text as supplier_code,
        'rest' as source_feed,
        1 as precedence
    from {{ ref('stg_rest_products') }}

    union all

    select
        sku,
        name,
        category,
        price_cents,
        null::text as currency,
        supplier_code,
        'file' as source_feed,
        2 as precedence
    from {{ ref('stg_file_products') }}
),

deduped as (
    select
        sku,
        name,
        category,
        price_cents,
        currency,
        supplier_code,
        source_feed,
        row_number() over (partition by sku order by precedence) as rn
    from unioned
)

select
    sku,
    name,
    category,
    price_cents,
    currency,
    supplier_code,
    source_feed
from deduped
where rn = 1
