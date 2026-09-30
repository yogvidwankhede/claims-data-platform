{{ config(meta={'row_access_column': 'line_of_business'}) }}

select
    f.rx_claim_id,
    f.member_id,
    mm.line_of_business,
    f.ndc,
    f.drug_name,
    f.fill_date,
    cast({{ dbt.date_trunc('month', 'f.fill_date') }} as date) as fill_month,
    f.days_supply,
    f.quantity,
    f.paid_amount,
    f.prescriber_npi
from {{ ref('stg_pharmacy_claims') }} as f
left join {{ ref('int_member_months') }} as mm
    on f.member_id = mm.member_id
    and cast({{ dbt.date_trunc('month', 'f.fill_date') }} as date) = mm.month_start
