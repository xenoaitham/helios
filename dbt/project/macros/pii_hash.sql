{#
    PII pseudonymization — ADR-008 D1, the ONE implementation of the contract
    in docs/DATA_DICTIONARY.md:

        SHA-256(lower(trim(value)) || <PII_HASH_SALT>) as lowercase hex

    - salt enters at parse time from the environment (env_var); it is never
      committed to the repo.
    - pgcrypto.digest() is ensured by the on-run-start hook in dbt_project.yml.
    - determinism is required: the same value must hash identically across
      feeds and runs (SCD2 change detection on identity fields, item 8).
#}
{% macro pii_hash(value_expr) -%}
    {%- if env_var('PII_HASH_SALT') == '' -%}
        {{ exceptions.raise_compiler_error(
            "PII_HASH_SALT is set but EMPTY — refusing to emit unsalted PII hashes (ADR-008 D1). Set it in .env."
        ) }}
    {%- endif -%}
encode(digest(lower(trim({{ value_expr }})) || '{{ env_var("PII_HASH_SALT") }}', 'sha256'), 'hex')
{%- endmacro %}
