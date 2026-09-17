-- Grain: one row per order line.
select
    order_item_id,
    order_id,
    sku,
    quantity,
    unit_price,
    quantity * unit_price as line_amount
from {{ source('raw', 'order_items') }}
