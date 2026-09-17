-- Grain: one row per order line.
--
-- The join to customers is the exposed surface. It assumes stg_customers holds
-- one row per customer_id. Nothing here enforces that, so a duplicate upstream
-- silently multiplies line_amount rather than failing the run.
select
    i.order_item_id,
    i.order_id,
    o.order_ref,
    o.customer_id,
    c.region_code,
    r.region_name,
    c.segment,
    o.order_date,
    o.status as order_status,
    o.channel,
    i.sku,
    i.quantity,
    i.unit_price,
    i.line_amount
from {{ ref('stg_order_items') }} i
join {{ ref('stg_orders') }} o
    on i.order_id = o.order_id
join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
left join {{ ref('stg_regions') }} r
    on c.region_code = r.region_code
