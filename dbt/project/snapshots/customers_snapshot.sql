{#
    customers_snapshot — the SCD2 store for dim_customer (ADR-009 D1).

    dbt snapshot, check strategy. Change detection is defined ONLY over the
    identity subset [email_hash, full_name_hash, country_code, is_active]:
    the mutator touches last_login_at ~40 users/tick, so including it would
    fabricate versions of pure noise (measured ground truth, ADR-009).
    Deterministic salted staging hashes (ADR-008 D1) make the check hash
    exact across builds.

    PERSISTENT STATE: history begins at the first run and must never be
    reset — `make dbt-build` runs plain `dbt build` and is NEVER wired with
    --full-refresh (a snapshot full-refresh drops history). The only
    sanctioned wipe is `make clean` (RUNBOOK).
#}
{% snapshot customers_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        unique_key = 'user_id',
        strategy = 'check',
        check_cols = ['email_hash', 'full_name_hash', 'country_code', 'is_active'],
        invalidate_hard_deletes = true
    )
}}

select
    user_id,
    email_hash,
    full_name_hash,
    country_code,
    is_active
from {{ ref('stg_users') }}

{% endsnapshot %}
