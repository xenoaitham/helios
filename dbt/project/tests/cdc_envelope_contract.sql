-- ADR-005 envelope contract, guarded at the staging boundary: a tombstone has
-- no after image and nothing else does. Any row returned = sink/Debezium drift
-- and staging's op<>'d' current-state read would be untrustworthy.
select 'cdc_users' as src, pk, lsn, op
from {{ source('helios_oltp', 'cdc_users') }}
where (op = 'd') is distinct from (after is null)
union all
select 'cdc_orders', pk, lsn, op
from {{ source('helios_oltp', 'cdc_orders') }}
where (op = 'd') is distinct from (after is null)
union all
select 'cdc_order_items', pk, lsn, op
from {{ source('helios_oltp', 'cdc_order_items') }}
where (op = 'd') is distinct from (after is null)
union all
select 'cdc_payments', pk, lsn, op
from {{ source('helios_oltp', 'cdc_payments') }}
where (op = 'd') is distinct from (after is null)
