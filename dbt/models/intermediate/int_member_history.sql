-- SCD2 member history with the first version stretched back to the start of time:
-- the snapshot only begins on the first load, but a member's plan before that is
-- best described by the earliest version we have.
with versions as (
    select
        member_id,
        line_of_business,
        zip3,
        birth_date,
        gender,
        coverage_start,
        coverage_end,
        cast(dbt_valid_from as date) as valid_from,
        cast(dbt_valid_to as date) as valid_to,
        row_number() over (partition by member_id order by dbt_valid_from) as version_number
    from {{ ref('snp_members') }}
)

select
    member_id,
    line_of_business,
    zip3,
    birth_date,
    gender,
    coverage_start,
    coverage_end,
    version_number,
    case when version_number = 1 then cast('1900-01-01' as date) else valid_from end as valid_from,
    valid_to
from versions
