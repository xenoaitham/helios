-- Batch landing is a 1:1 keyed upsert (ADR-007), stable between ingest runs:
-- each batch staging model must carry exactly its raw table's rows.
-- CDC parity is deliberately NOT asserted here — the live mutator makes strict
-- raw↔staging count equality racy by construction; VERIFIER evidences it with
-- drift attribution instead (ADR-008 D6).
with parity as (
    select 'stg_soap_orders' as model_name,
           (select count(*) from {{ source('helios_soap', 'soap_orders') }}) as raw_rows,
           (select count(*) from {{ ref('stg_soap_orders') }}) as stg_rows
    union all
    select 'stg_file_customers',
           (select count(*) from {{ source('helios_file', 'file_customers') }}),
           (select count(*) from {{ ref('stg_file_customers') }})
    union all
    select 'stg_file_products',
           (select count(*) from {{ source('helios_file', 'file_products') }}),
           (select count(*) from {{ ref('stg_file_products') }})
    union all
    select 'stg_rest_products',
           (select count(*) from {{ source('helios_rest', 'rest_products') }}),
           (select count(*) from {{ ref('stg_rest_products') }})
    union all
    select 'stg_rest_promotions',
           (select count(*) from {{ source('helios_rest', 'rest_promotions') }}),
           (select count(*) from {{ ref('stg_rest_promotions') }})
)
select * from parity where raw_rows <> stg_rows
