
# 1086 (25%)
select policy_id, policy_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-06-30'
and carrier <> 'Canopius'
and state not in ('TX', 'CA')
and product <> 'HO5'
and calculated_fields_non_cat_risk_class = 'no_action'