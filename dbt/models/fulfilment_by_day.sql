select
    load_date,
    warehouse_id,
    event_type,
    count(*) as events
from {{ source('wh', 'fulfilment_events') }}
group by load_date, warehouse_id, event_type
