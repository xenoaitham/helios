-- fct_order_items — the line-grain fact (ADR-009 D4/D6): OLTP-only by
-- construction (SOAP has no items feed). The order-level dimensions resolved
-- in fct_orders (customer_sk via the SCD2 point-in-time join, order_ts,
-- order_date_key) are carried onto the line grain by joining fct_orders on
-- the composite — NOT re-resolved, so both facts agree per order. product_sku
-- joins dim_product LEFT: the catalog covers 6,895 of 100,000 item SKUs
-- (~6.89% of rows by seed design); unmatched lines keep NULL product
-- attributes and row parity is tested exactly — never an inner join.
{{ config(materialized='table', schema='marts') }}

select
    i.order_item_id,
    'oltp'                                       as source_type,
    i.order_id,
    o.customer_sk,
    o.order_ts,
    o.order_date_key,
    i.product_sku,
    p.source_feed                                as product_source_feed,
    i.quantity,
    i.unit_price,
    i.quantity * i.unit_price                    as line_revenue
from {{ ref('stg_order_items') }} i
left join {{ ref('fct_orders') }} o
    on o.source_type = 'oltp'
   and o.order_id = i.order_id
left join {{ ref('dim_product') }} p
    on p.sku = i.product_sku
