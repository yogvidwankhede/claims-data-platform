{{
    config(
        materialized='incremental',
        unique_key='claim_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- Incremental: each run reprocesses claims from the last `lookback_days` of loads.
-- delete+insert on claim_id replaces the whole claim, so an adjustment that changes
-- a claim's lines or amounts never leaves the old version behind.
select
    claim_id,
    claim_version,
    claim_status,
    claim_type,
    setting,
    member_id,
    billing_npi,
    service_from,
    service_to,
    cast({{ dbt.date_trunc('month', 'service_from') }} as date) as service_month,
    admit_date,
    discharge_date,
    primary_dx,
    secondary_dx,
    cast(line_count as bigint) as line_count,
    cast(billed_amount as decimal(18, 2)) as billed_amount,
    cast(allowed_amount as decimal(18, 2)) as allowed_amount,
    cast(net_paid_amount as decimal(18, 2)) as net_paid_amount,
    paid_date,
    batch_date
from {{ ref('int_claims') }}
{% if is_incremental() %}
where batch_date >= (
    select {{ dbt.dateadd('day', -var('lookback_days'), 'max(batch_date)') }} from {{ this }}
)
{% endif %}
