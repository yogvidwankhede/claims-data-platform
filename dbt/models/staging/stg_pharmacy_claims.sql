select
    rx_claim_id,
    member_id,
    ndc,
    drug_name,
    cast(fill_date as date) as fill_date,
    cast(days_supply as integer) as days_supply,
    cast(quantity as integer) as quantity,
    cast(paid_amount as decimal(18, 2)) as paid_amount,
    prescriber_npi,
    cast(_batch_date as date) as batch_date
from {{ source('raw', 'pharmacy_claims') }}
