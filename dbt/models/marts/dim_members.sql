{{ config(meta={'row_access_column': 'line_of_business'}) }}

with current_version as (
    select *
    from {{ ref('int_member_history') }}
    where valid_to = cast('9999-12-31' as date)
),

as_of as (
    select max(roster_date) as as_of_date from {{ ref('stg_members') }}
)

select
    c.member_id,
    c.line_of_business,
    c.zip3,
    c.gender,
    c.birth_date,
    cast(floor({{ dbt.datediff('c.birth_date', 'a.as_of_date', 'day') }} / 365.25) as integer) as age,
    case
        when {{ dbt.datediff('c.birth_date', 'a.as_of_date', 'day') }} / 365.25 < 18 then '0-17'
        when {{ dbt.datediff('c.birth_date', 'a.as_of_date', 'day') }} / 365.25 < 45 then '18-44'
        when {{ dbt.datediff('c.birth_date', 'a.as_of_date', 'day') }} / 365.25 < 65 then '45-64'
        else '65+'
    end as age_band,
    c.coverage_start,
    c.coverage_end,
    cast(c.version_number as integer) as history_versions
from current_version as c
cross join as_of as a
