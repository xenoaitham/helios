-- Orphan line integrity at the published layer (ADR-009 D6): every
-- fct_order_items row must find its parent order in fct_orders on the
-- composite (source_type='oltp', order_id). The composite matters — the
-- OLTP and SOAP order_id namespaces collide numerically, so a bare id join
-- would false-match a SOAP order for a straddled item. Staging warns on this
-- edge (ADR-008 D3); the mart FAILS the build loudly — the orchestrator
-- retries, a mart never publishes unreconciled rows.
select
    i.order_item_id,
    i.order_id
from {{ ref('fct_order_items') }} i
left join {{ ref('fct_orders') }} o
    on o.source_type = 'oltp'
   and o.order_id = i.order_id
where o.order_id is null
