-- Every paid inpatient stay as a potential index admission, flagged when the same
-- member is admitted again within the window after discharge (all-cause; planned
-- readmissions are not excluded). `window_complete` is false while the follow-up
-- window is still open, and such stays are left out of the rates.
with stays as (
    select * from {{ ref('int_inpatient_stays') }}
),

data_end as (
    select max(service_from) as last_service_date from {{ ref('int_claims') }}
),

next_admit as (
    select
        idx.claim_id,
        min(nxt.admit_date) as next_admit_date
    from stays as idx
    inner join stays as nxt
        on nxt.member_id = idx.member_id
        and nxt.claim_id <> idx.claim_id
        and nxt.admit_date > idx.discharge_date
        and nxt.admit_date <= {{ dbt.dateadd('day', var('readmission_window_days'), 'idx.discharge_date') }}
    group by idx.claim_id
)

select
    s.claim_id as index_claim_id,
    s.member_id,
    s.facility_npi,
    s.admit_date,
    s.discharge_date,
    cast({{ dbt.date_trunc('month', 's.discharge_date') }} as date) as discharge_month,
    cast(s.length_of_stay as integer) as length_of_stay,
    s.primary_dx,
    n.next_admit_date,
    n.next_admit_date is not null as readmitted_within_window,
    {{ dbt.dateadd('day', var('readmission_window_days'), 's.discharge_date') }} <= d.last_service_date
        as window_complete
from stays as s
cross join data_end as d
left join next_admit as n on s.claim_id = n.claim_id
