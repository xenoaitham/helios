-- fct_orders — the order-grain fact: legacy SOAP + modern OLTP in ONE governed
-- star, discriminated by source_type (ADR-009 D3). Grain is the composite
-- (source_type, order_id) — the numeric id namespaces collide, so uniqueness
-- is composite-tested. SOAP customers carry the Kimball unknown member
-- (customer_sk IS NULL ⇔ source_type='soap', tested both directions): SOAP
-- orders have no identity attribute, so dim_customer stays OLTP-only.
--
-- Point-in-time SCD2 resolution (ADR-009 D2): the fact carries the natural key
-- and resolves the dim_customer surrogate whose half-open window
-- [valid_from, valid_to) contains order_ts; orders older than snapshot
-- history fall back to the customer's earliest known version (documented
-- limitation — history begins at the snapshot's first run).
{{ config(materialized='table', schema='marts') }}

with oltp_orders as (
    select
        'oltp'                                   as source_type,
        order_id,
        user_id                                  as customer_id,
        status,
        currency,
        total_amount,
        placed_at                                as order_ts,
        (placed_at at time zone 'UTC')::date     as order_date
    from {{ ref('stg_orders') }}
),

soap_orders as (
    select
        'soap'                                   as source_type,
        order_id,
        customer_id,
        status,
        currency,
        total_amount,
        created_at                               as order_ts,
        (created_at at time zone 'UTC')::date    as order_date
    from {{ ref('stg_soap_orders') }}
),

orders as (
    select * from oltp_orders
    union all
    select * from soap_orders
),

-- earliest known version per customer: the pre-history fallback (D2)
first_version as (
    select distinct on (customer_id)
        customer_id,
        customer_sk
    from {{ ref('dim_customer') }}
    order by customer_id, valid_from
)

select
    o.source_type,
    o.order_id,
    o.customer_id,
    coalesce(pit.customer_sk, fv.customer_sk)    as customer_sk,
    o.status,
    o.currency,
    o.total_amount,
    o.order_ts,
    to_char(o.order_date, 'YYYYMMDD')::integer   as order_date_key
from orders o
left join {{ ref('dim_customer') }} pit
    on o.source_type = 'oltp'
   and pit.customer_id = o.customer_id
   and o.order_ts >= pit.valid_from
   and (pit.valid_to is null or o.order_ts < pit.valid_to)
left join first_version fv
    on o.source_type = 'oltp'
   and fv.customer_id = o.customer_id
