-- Grain: one row per claim.
select
    claim_id,
    claim_ref,
    policy_id,
    claim_date,
    claim_type,
    amount,
    status
from {{ source('raw', 'claims') }}
