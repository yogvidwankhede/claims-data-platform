select
    npi,
    provider_name,
    specialty,
    state,
    is_facility
from {{ ref('stg_providers') }}
