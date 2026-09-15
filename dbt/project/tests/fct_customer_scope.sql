-- Unknown-member discipline (ADR-009 D3): customer_sk IS NULL exactly when
-- the row is a SOAP order — both directions, no exceptions.
select
    'oltp_row_without_customer_sk' as violation,
    source_type,
    order_id
from {{ ref('fct_orders') }}
where source_type = 'oltp'
  and customer_sk is null

union all

select
    'soap_row_with_customer_sk' as violation,
    source_type,
    order_id
from {{ ref('fct_orders') }}
where source_type = 'soap'
  and customer_sk is not null
