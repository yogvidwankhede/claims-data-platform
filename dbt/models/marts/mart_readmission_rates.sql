{{ config(meta={'row_access_column': 'line_of_business'}) }}

-- 30-day all-cause readmission rate by discharge month and line of business,
-- over index stays whose follow-up window has closed.
select
    r.discharge_month,
    r.line_of_business,
    cast(count(*) as bigint) as index_stays,
    cast(sum(case when r.readmitted_within_window then 1 else 0 end) as bigint) as readmissions,
    cast(avg(case when r.readmitted_within_window then 1.0 else 0.0 end) as decimal(6, 4)) as readmission_rate
from {{ ref('fct_readmissions') }} as r
where r.window_complete
group by r.discharge_month, r.line_of_business
