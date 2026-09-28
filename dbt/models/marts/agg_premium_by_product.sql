select
    product_code,
    status,
    count(*) as policies,
    count(distinct person_id) as policyholders,
    sum(annual_premium) as gross_written_premium
from {{ ref('fct_policies') }}
group by 1, 2
