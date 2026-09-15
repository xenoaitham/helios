-- dim_date — generated calendar, day grain, spanning the full order-timeline
-- range (OLTP placed_at ∪ SOAP created_at; ADR-009 D5). Built from staging so
-- the DAG stays staging → dims → facts; fct coverage is tested. date_key is
-- the classic YYYYMMDD integer; days are UTC (order dates are UTC-derived).
{{ config(materialized='table', schema='marts') }}

with bounds as (
    select min(ts) as lo, max(ts) as hi
    from (
        select placed_at as ts from {{ ref('stg_orders') }}
        union all
        select created_at as ts from {{ ref('stg_soap_orders') }}
    ) timeline
),

days as (
    select
        generate_series(
            date_trunc('day', lo),
            date_trunc('day', hi),
            interval '1 day'
        )::date as date_day
    from bounds
)

select
    to_char(date_day, 'YYYYMMDD')::integer as date_key,
    date_day,
    extract(year  from date_day)::integer as year_number,
    extract(quarter from date_day)::integer as quarter_number,
    extract(month from date_day)::integer as month_number,
    trim(to_char(date_day, 'Month'))      as month_name,
    extract(day   from date_day)::integer as day_number,
    extract(isodow from date_day)::integer as iso_day_of_week,
    trim(to_char(date_day, 'Day'))        as day_name,
    (extract(isodow from date_day) >= 6)  as is_weekend
from days
