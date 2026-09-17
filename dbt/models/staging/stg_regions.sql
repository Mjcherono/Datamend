select
    region_code,
    region_name,
    country
from {{ source('raw', 'regions') }}
