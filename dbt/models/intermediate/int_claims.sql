-- Claim header: one row per claim (current version), rolled up from its lines.
select
    claim_id,
    max(claim_version) as claim_version,
    max(claim_status) as claim_status,
    max(claim_type) as claim_type,
    max(member_id) as member_id,
    max(billing_npi) as billing_npi,
    max(setting) as setting,
    min(service_from) as service_from,
    max(service_to) as service_to,
    max(admit_date) as admit_date,
    max(discharge_date) as discharge_date,
    max(primary_dx) as primary_dx,
    max(secondary_dx) as secondary_dx,
    count(*) as line_count,
    sum(billed_amount) as billed_amount,
    sum(allowed_amount) as allowed_amount,
    sum(net_paid_amount) as net_paid_amount,
    max(paid_date) as paid_date,
    max(batch_date) as batch_date
from {{ ref('stg_medical_claim_lines') }}
group by claim_id
