select
    load_date,
    count(*) as orders,
    count(*) filter (where status = 'cancelled') as cancelled_orders,
    sum(total_amount) filter (where status <> 'cancelled') as revenue_gbp
from {{ source('wh', 'orders') }}
group by load_date
