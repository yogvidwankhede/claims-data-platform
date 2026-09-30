select
    npi,
    provider_name,
    specialty,
    state,
    cast(is_facility as boolean) as is_facility
from {{ source('raw', 'providers') }}
