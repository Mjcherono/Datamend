-- Grain: one row per policyholder.
select
    person_id,
    person_ref,
    first_name,
    last_name,
    email,
    date_of_birth,
    address_line,
    county,
    eircode,
    phone
from {{ source('raw', 'policyholders') }}
