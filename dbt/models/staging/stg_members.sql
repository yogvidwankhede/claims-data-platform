-- Names stay in RAW: nothing downstream needs them (minimum necessary, HIPAA).
select
    member_id,
    cast(birth_date as date) as birth_date,
    gender,
    zip3,
    line_of_business,
    cast(coverage_start as date) as coverage_start,
    cast(coverage_end as date) as coverage_end,
    cast(_batch_date as date) as roster_date,
    cast(_batch_date as timestamp) as roster_effective_at
from {{ source('raw', 'members') }}
