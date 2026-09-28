-- Grain: one row per policy, joined to the holder.
select
    p.policy_id,
    p.policy_ref,
    p.person_id,
    h.county,
    p.product_code,
    p.cover_start,
    p.cover_end,
    p.status,
    p.days_since_cover_ended,
    p.annual_premium
from {{ ref('stg_policies') }} p
join {{ ref('stg_policyholders') }} h
    on p.person_id = h.person_id
