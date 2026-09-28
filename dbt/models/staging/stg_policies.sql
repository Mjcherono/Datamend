-- Grain: one row per policy.
select
    policy_id,
    policy_ref,
    person_id,
    product_code,
    cover_start,
    cover_end,
    status,
    annual_premium,
    datediff('day', cover_end, current_date) as days_since_cover_ended
from {{ source('raw', 'policies') }}
