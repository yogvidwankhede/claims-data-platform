-- Upstream guarantees the current table holds only each claim's latest version.
-- If lines of one claim ever disagree on version or status, the header rollup is wrong.
select claim_id
from {{ ref('stg_medical_claim_lines') }}
group by claim_id
having count(distinct claim_version) > 1 or count(distinct claim_status) > 1
