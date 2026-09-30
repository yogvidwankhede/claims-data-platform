{{
    config(
        materialized='incremental',
        unique_key='claim_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        meta={'row_access_column': 'line_of_business'}
    )
}}

-- Incremental: each run reprocesses claims from the last `lookback_days` of loads.
-- delete+insert on claim_id replaces the whole claim, so an adjustment that changes
-- a claim's lines or amounts never leaves the old version behind.
with claims as (
    select * from {{ ref('int_claims') }}
    {% if is_incremental() %}
    where batch_date >= (
        select {{ dbt.dateadd('day', -var('lookback_days'), 'max(batch_date)') }} from {{ this }}
    )
    {% endif %}
)

select
    c.claim_id,
    c.claim_version,
    c.claim_status,
    c.claim_type,
    c.setting,
    c.member_id,
    -- the plan the member was in that month: drives row access and every by-plan metric
    mm.line_of_business,
    c.billing_npi,
    c.service_from,
    c.service_to,
    cast({{ dbt.date_trunc('month', 'c.service_from') }} as date) as service_month,
    c.admit_date,
    c.discharge_date,
    c.primary_dx,
    c.secondary_dx,
    cast(c.line_count as bigint) as line_count,
    cast(c.billed_amount as decimal(18, 2)) as billed_amount,
    cast(c.allowed_amount as decimal(18, 2)) as allowed_amount,
    cast(c.net_paid_amount as decimal(18, 2)) as net_paid_amount,
    c.paid_date,
    c.batch_date
from claims as c
left join {{ ref('int_member_months') }} as mm
    on c.member_id = mm.member_id
    and cast({{ dbt.date_trunc('month', 'c.service_from') }} as date) = mm.month_start
