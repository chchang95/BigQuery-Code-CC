select id, policy_number, json_extract_scalar(coalesce(policy_info,transaction),'$.property_data.purchase_date')
from postgres_public.policies 
where bound = true
and state = 'ca'
and carrier = 'topa'
-- LIMIT 100