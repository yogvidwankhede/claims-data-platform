-- Paid inpatient admissions (denied and reversed claims are not stays).
select
    claim_id,
    member_id,
    billing_npi as facility_npi,
    admit_date,
    discharge_date,
    {{ dbt.datediff('admit_date', 'discharge_date', 'day') }} as length_of_stay,
    primary_dx,
    net_paid_amount
from {{ ref('int_claims') }}
where setting = 'inpatient'
    and claim_status = 'PAID'
    and admit_date is not null
    and discharge_date is not null
