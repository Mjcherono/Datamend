-- Grain: one row per order.
select
    order_id,
    order_ref,
    customer_id,
    order_date,
    status,
    channel
from {{ source('raw', 'orders') }}
