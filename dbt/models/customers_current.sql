select customer_id, email, full_name, tier, country, marketing_opt_in, valid_from as current_since
from {{ source('wh', 'customer_history') }}
where is_current
