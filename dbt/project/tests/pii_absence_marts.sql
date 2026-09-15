-- Cleartext-leftover detector at the mart boundary (ADR-009 D7): every hash
-- column in the marts AND the snapshot must look like a hash, never like the
-- value it replaced — emails contain '@', names contain a space. The
-- snapshot is swept too: state tables are as consumer-sensitive as marts.
select
    'dim_customer.email_hash' as column_checked,
    customer_sk as offending_key
from {{ ref('dim_customer') }}
where email_hash like '%@%' or email_hash like '% %'

union all

select
    'dim_customer.full_name_hash',
    customer_sk
from {{ ref('dim_customer') }}
where full_name_hash like '%@%' or full_name_hash like '% %'

union all

select
    'customers_snapshot.email_hash',
    dbt_scd_id
from {{ ref('customers_snapshot') }}
where email_hash like '%@%' or email_hash like '% %'

union all

select
    'customers_snapshot.full_name_hash',
    dbt_scd_id
from {{ ref('customers_snapshot') }}
where full_name_hash like '%@%' or full_name_hash like '% %'
