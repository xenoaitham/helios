-- Promotions must reference the shared product catalog (rest ∪ file SKUs).
-- Measured total at Phase 3 open (0 missing of 2,500); error severity.
-- oltp items→catalog is deliberately NOT tested: the items namespace is wider
-- than the catalog by seed design (100,000 distinct SKUs, 6,895 covered) —
-- documented in ADR-008 D6 / DATA_DICTIONARY.
select p.promotion_id, p.product_sku
from {{ ref('stg_rest_promotions') }} p
left join (
    select sku from {{ ref('stg_rest_products') }}
    union
    select sku from {{ ref('stg_file_products') }}
) catalog on catalog.sku = p.product_sku
where catalog.sku is null
