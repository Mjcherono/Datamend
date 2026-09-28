-- Grain: one row per policyholder.
--
-- Carries the identifying fields, so this is the first place an erased subject
-- becomes visible again if they return upstream.
select
    person_id,
    person_ref,
    first_name || ' ' || last_name as full_name,
    email,
    date_of_birth,
    county,
    eircode,
    phone
from {{ ref('stg_policyholders') }}
