select
    rx_claim_id,
    member_id,
    ndc,
    drug_name,
    fill_date,
    cast({{ dbt.date_trunc('month', 'fill_date') }} as date) as fill_month,
    days_supply,
    quantity,
    paid_amount,
    prescriber_npi
from {{ ref('stg_pharmacy_claims') }}
