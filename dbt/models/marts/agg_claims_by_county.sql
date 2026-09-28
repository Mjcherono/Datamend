-- An aggregate carries no names, which is exactly why an erased subject can
-- survive here unnoticed long after the detail tables were cleaned.
select
    county,
    count(distinct person_id) as claimants,
    count(*) as claims,
    sum(amount) as claim_cost
from {{ ref('fct_claims') }}
group by 1
