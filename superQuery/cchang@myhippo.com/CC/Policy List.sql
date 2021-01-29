with pol as (
select eps.policy_id
,status
-- ,eps.is_status_active
-- ,date_cancellation
,dp.policy_number
,date_policy_effective as effective_date
,date_policy_expires as expiration_date
,property_data_address_street as street
,property_data_address_city as city
,property_data_address_zip as zip
,property_data_address_county as county
,state 
,product 
,property_data_protection_class
,JSON_EXTRACT_SCALAR(property_data_zillow,'$.zestimate') as zillow_market_value
,property_data_zillow
,calculated_fields_market_value_much_below_rce
,prefilled_rebuilding_cost
-- ,JSON_EXTRACT_SCALAR(prefilled_zillow
-- ,zestim
from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, policy_number from dw_prod.dim_policies) dp USING(policy_id)
left join dw_prod.fct_premium_updates fpu on eps.latest_policy_update_id = fpu.policy_update_id
where date_snapshot = '2020-12-31'
-- and date_policy_effective >= '2020-07-01'
-- and carrier <> 'Canopius'
and product <> 'ho5'
-- and status = 'active'
-- and carrier = 'spinnaker'
-- and state = 'ca'
-- and state = 'tx'
-- and property_data_address_zip = '78332'
-- and calculated_fields_wind_exclusion <> 'true'
-- and date_policy_effective <= '2020-05-31'
)
select count(*) from pol
-- where policy_number = 'HAZ-1348250-00'