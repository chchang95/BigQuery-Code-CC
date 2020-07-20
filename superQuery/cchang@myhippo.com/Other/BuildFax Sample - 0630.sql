with policy_claims as (
# 13338
select distinct policy_id, policy_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
, 'c' as type
from dw_prod_extracts.ext_policy_snapshots
left join (select policy_id from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = '2020-06-30') claims using(policy_id)
where date_snapshot = '2020-06-30'
and claims.policy_id is not null
and carrier <> 'Canopius'
and product <> 'HO5'
), policy_referrals as (
# 1432
select policy_id, policy_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
, 'r' as type
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-06-30'
and carrier <> 'Canopius'
and state not in ('TX', 'CA')
and product <> 'HO5'
and calculated_fields_non_cat_risk_class = 'referral'
), policy_interior as (
# 1665 (25%)
select policy_id, policy_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
, 'i' as type
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-06-30'
and carrier <> 'Canopius'
and state not in ('TX', 'CA')
and product <> 'HO5'
and calculated_fields_non_cat_risk_class = 'interior_inspection_required'
order by street asc
LIMIT 1665
), policy_exterior as (
# 1665 (25%)
select policy_id, policy_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
, 'e' as type
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-06-30'
and carrier <> 'Canopius'
and state not in ('TX', 'CA')
and product <> 'HO5'
and calculated_fields_non_cat_risk_class = 'exterior_inspection_required'
order by street asc
LIMIT 1665
), policy_no_action as (
# 1900 (25%)
select policy_id, policy_number
, property_data_address_street as street
, property_data_address_city as city
, property_data_address_zip as zip
, property_data_address_state as state
, 'h' as type
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-06-30'
and carrier <> 'Canopius'
and state not in ('TX', 'CA')
and product <> 'HO5'
and calculated_fields_non_cat_risk_class = 'no_action'
order by street asc
LIMIT 1900
)
select * from policy_claims
union all
select * from policy_referrals
union all
select * from policy_interior
union all
select * from policy_exterior
union all
select * from policy_no_action