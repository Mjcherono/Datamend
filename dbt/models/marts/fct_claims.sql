-- Grain: one row per claim, resolved to the person who made it.
select
    c.claim_id,
    c.claim_ref,
    c.policy_id,
    p.person_id,
    p.county,
    p.product_code,
    c.claim_date,
    c.claim_type,
    c.amount,
    c.status
from {{ ref('stg_claims') }} c
join {{ ref('fct_policies') }} p
    on c.policy_id = p.policy_id
