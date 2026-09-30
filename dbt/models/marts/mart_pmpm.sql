{{ config(meta={'row_access_column': 'line_of_business'}) }}

-- Per-member-per-month cost by line of business. Claims and fills are attributed
-- to the member-month they occurred in, so a member who changed plans mid-year
-- counts toward each plan for the months they were in it.
--
-- Recent months are incomplete until their claims run out (claims arrive weeks
-- after service): `runout_days` says how long each month has had, and finance
-- reads a month as settled only past ~90 days.
with member_months as (
    select month_start, line_of_business, member_id from {{ ref('int_member_months') }}
),

medical as (
    select mm.month_start, mm.line_of_business, sum(c.net_paid_amount) as medical_paid
    from {{ ref('fct_claims') }} as c
    inner join member_months as mm
        on c.member_id = mm.member_id and c.service_month = mm.month_start
    group by mm.month_start, mm.line_of_business
),

pharmacy as (
    select mm.month_start, mm.line_of_business, sum(f.paid_amount) as pharmacy_paid
    from {{ ref('fct_pharmacy_fills') }} as f
    inner join member_months as mm
        on f.member_id = mm.member_id and f.fill_month = mm.month_start
    group by mm.month_start, mm.line_of_business
),

runout as (
    select max(paid_date) as latest_paid_date from {{ ref('fct_claims') }}
),

denominator as (
    select month_start, line_of_business, count(*) as member_months
    from member_months
    group by month_start, line_of_business
)

select
    d.month_start,
    d.line_of_business,
    cast(d.member_months as bigint) as member_months,
    cast(coalesce(m.medical_paid, 0) as decimal(18, 2)) as medical_paid,
    cast(coalesce(p.pharmacy_paid, 0) as decimal(18, 2)) as pharmacy_paid,
    cast(coalesce(m.medical_paid, 0) / d.member_months as decimal(18, 2)) as medical_pmpm,
    cast(coalesce(p.pharmacy_paid, 0) / d.member_months as decimal(18, 2)) as pharmacy_pmpm,
    cast((coalesce(m.medical_paid, 0) + coalesce(p.pharmacy_paid, 0)) / d.member_months as decimal(18, 2))
        as total_pmpm,
    cast({{ dbt.datediff(dbt.last_day('d.month_start', 'month'), 'r.latest_paid_date', 'day') }} as integer)
        as runout_days
from denominator as d
cross join runout as r
left join medical as m on d.month_start = m.month_start and d.line_of_business = m.line_of_business
left join pharmacy as p on d.month_start = p.month_start and d.line_of_business = p.line_of_business
