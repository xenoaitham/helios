{#
    Generic test: every non-null value must match a POSIX regex. Used for the
    PII hash-format contract (64 lowercase hex chars, ADR-008 D1) without
    pulling in dbt_utils (the project is package-free by design).
#}
{% test values_match_regex(model, column_name, pattern) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} !~ '{{ pattern }}'
{% endtest %}
