select policy_id, policy_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
from dw_prod_extracts.ext_policy_snapshots
left join (select policy_id from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = '2020-06-30') claims using(policy_id)
where date_snapshot = '2020-06-30'
and claims.policy_id is not null
and carrier <> 'Canopius'
and product <> 'HO5'