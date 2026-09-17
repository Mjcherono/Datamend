-- Grain: one row per customer.
select
    customer_id,
    customer_ref,
    region_code,
    segment,
    signup_date,
    status
from {{ source('raw', 'customers') }}
