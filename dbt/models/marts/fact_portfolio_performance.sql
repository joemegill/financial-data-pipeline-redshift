with positions as (
    select * from {{ ref('stg_etrade_positions') }}
)
select
    symbol,
    sum(position_value) as total_value
from positions
group by symbol
