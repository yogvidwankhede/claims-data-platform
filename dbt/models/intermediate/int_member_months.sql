-- One row per member per month of enrolment: the denominator of every PMPM and
-- per-1000 metric. Convention: a member counts for a month if covered on any day of
-- it (so every claim lands in a member-month), attributed to the line of business in
-- force at the start of the month. Months span the claims history we hold; enrolment
-- before it would add denominator with no claims.
with bounds as (
    select
        cast({{ dbt.date_trunc('month', 'min(service_from)') }} as date) as first_month,
        cast({{ dbt.date_trunc('month', 'max(service_from)') }} as date) as last_month
    from {{ ref('stg_medical_claim_lines') }}
),

-- a fixed series filtered to the bounds keeps this plain SQL on every warehouse
-- (and mockable in unit tests); 600 months is 50 years of claims history
offsets as (
    {{ dbt.generate_series(600) }}
),

month_starts as (
    select cast({{ dbt.dateadd('month', 'o.generated_number - 1', 'b.first_month') }} as date) as month_start
    from offsets as o
    cross join bounds as b
    where {{ dbt.dateadd('month', 'o.generated_number - 1', 'b.first_month') }} <= b.last_month
)

select
    h.member_id,
    m.month_start,
    h.line_of_business,
    h.zip3,
    h.birth_date,
    h.gender
from month_starts as m
inner join {{ ref('int_member_history') }} as h
    on h.coverage_start <= {{ dbt.last_day('m.month_start', 'month') }}
    and (h.coverage_end is null or h.coverage_end >= m.month_start)
    and m.month_start >= h.valid_from
    and m.month_start < h.valid_to
