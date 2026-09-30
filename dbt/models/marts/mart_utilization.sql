{{ config(meta={'row_access_column': 'line_of_business'}) }}

-- Utilization per 1,000 member-years: events / member months * 12,000 (the
-- annualized rate payers and CMS report). Denied and reversed claims are not events.
with member_months as (
    select month_start, line_of_business, member_id from {{ ref('int_member_months') }}
),

events as (
    select
        mm.month_start,
        mm.line_of_business,
        sum(case when c.setting = 'inpatient' then 1 else 0 end) as inpatient_admits,
        sum(case when c.setting = 'emergency' then 1 else 0 end) as er_visits,
        sum(case when c.setting = 'office' then 1 else 0 end) as office_visits
    from {{ ref('fct_claims') }} as c
    inner join member_months as mm
        on c.member_id = mm.member_id and c.service_month = mm.month_start
    where c.claim_status = 'PAID'
    group by mm.month_start, mm.line_of_business
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
    cast(coalesce(e.inpatient_admits, 0) as bigint) as inpatient_admits,
    cast(coalesce(e.er_visits, 0) as bigint) as er_visits,
    cast(coalesce(e.office_visits, 0) as bigint) as office_visits,
    cast(coalesce(e.inpatient_admits, 0) * 12000.0 / d.member_months as decimal(18, 1)) as admits_per_1000,
    cast(coalesce(e.er_visits, 0) * 12000.0 / d.member_months as decimal(18, 1)) as er_visits_per_1000,
    cast(coalesce(e.office_visits, 0) * 12000.0 / d.member_months as decimal(18, 1)) as office_visits_per_1000
from denominator as d
left join events as e on d.month_start = e.month_start and d.line_of_business = e.line_of_business
