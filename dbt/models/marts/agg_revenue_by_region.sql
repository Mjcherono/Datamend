select
    region_code,
    region_name,
    count(distinct order_id) as orders,
    count(*) as order_lines,
    sum(line_amount) as gross_revenue
from {{ ref('fct_order_items') }}
where order_status = 'complete'
group by 1, 2
