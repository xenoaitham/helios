-- Same-run row parity (ADR-009 D6): marts never gain or lose rows vs the
-- staging they were built from. Deterministic by construction — tests read
-- the built relations and staging is static within a run (ADR-008 D3).
--   fct_orders       = stg_orders + stg_soap_orders   (union, no drops)
--   fct_order_items  = stg_order_items                (LEFT joins, no drops)
--   dim_product      = rest ∪ file catalog union      (dedupe, no drops)
--   dim_customer     current rows = stg_users          (SCD2 seeds everyone;
--                      hard-deleted users would close their window — none exist)
with parity as (
    select
        'fct_orders' as mart,
        (select count(*) from {{ ref('stg_orders') }})
            + (select count(*) from {{ ref('stg_soap_orders') }}) as expected,
        (select count(*) from {{ ref('fct_orders') }}) as actual
    union all
    select
        'fct_order_items',
        (select count(*) from {{ ref('stg_order_items') }}),
        (select count(*) from {{ ref('fct_order_items') }})
    union all
    select
        'dim_product',
        (
            select count(*)
            from (
                select sku from {{ ref('stg_rest_products') }}
                union
                select sku from {{ ref('stg_file_products') }}
            ) catalog
        ),
        (select count(*) from {{ ref('dim_product') }})
    union all
    select
        'dim_customer_current',
        (select count(*) from {{ ref('stg_users') }}),
        (select count(*) from {{ ref('dim_customer') }} where is_current)
)
select * from parity where actual <> expected
