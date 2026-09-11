{#
    Generic test: composite key uniqueness, package-free (ADR-009 D3).
    fct_orders grain is (source_type, order_id) — OLTP and SOAP order_id
    namespaces collide numerically, so single-column unique would be wrong.
#}
{% test unique_combination(model, columns) %}

select
    {{ columns | join(', ') }} as composite_key,
    count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
