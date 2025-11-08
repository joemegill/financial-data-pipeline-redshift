select
    symbol,
    cast(quantity as integer) as quantity,
    cast(price as double precision) as price,
    cast(quantity as integer) * cast(price as double precision) as position_value
from {{ source('raw', 'etrade_positions') }}
