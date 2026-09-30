{#- Paid medical dollars in the PMPM mart must equal paid claims in covered months.
    A gap means claims fell outside every member-month (enrolment or attribution bug). -#}
{% test pmpm_reconciles_to_claims(model) %}
with mart as (
    select sum(medical_paid) as paid from {{ model }}
),

claims as (
    select sum(net_paid_amount) as paid
    from {{ ref('fct_claims') }}
    where service_month >= (select min(month_start) from {{ model }})
)

select mart.paid as mart_paid, claims.paid as claims_paid
from mart cross join claims
where abs(coalesce(claims.paid, 0) - coalesce(mart.paid, 0)) > 0.005 * coalesce(claims.paid, 0)
{% endtest %}
