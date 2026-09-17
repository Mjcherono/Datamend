select
    segment,
    count(distinct customer_id) as customers,
    count(distinct order_id) as orders,
    sum(line_amount) as gross_revenue
from {{ ref('fct_order_items') }}
where order_status = 'complete'
group by 1
