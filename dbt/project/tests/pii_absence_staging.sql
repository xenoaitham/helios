-- Cleartext-leftover detector (ADR-008 D1): hash columns must never look like
-- the values they replaced — emails contain '@', names contain a space. If any
-- row returns, someone projected cleartext PII into staging.
select 'stg_users.email_hash' as column_checked, user_id as offending_key
from {{ ref('stg_users') }}
where email_hash like '%@%' or email_hash like '% %'
union all
select 'stg_users.full_name_hash', user_id
from {{ ref('stg_users') }}
where full_name_hash like '%@%' or full_name_hash like '% %'
union all
select 'stg_file_customers.email_hash', customer_id
from {{ ref('stg_file_customers') }}
where email_hash like '%@%' or email_hash like '% %'
union all
select 'stg_file_customers.full_name_hash', customer_id
from {{ ref('stg_file_customers') }}
where full_name_hash like '%@%' or full_name_hash like '% %'
