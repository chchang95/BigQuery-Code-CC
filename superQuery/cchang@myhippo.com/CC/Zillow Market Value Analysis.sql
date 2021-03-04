with pol as (
select eps.policy_id
,status
,dp.policy_number
,date_policy_effective as effective_date
,date_policy_expires as expiration_date
,property_data_address_street as street
,property_data_address_city as city
,property_data_address_zip as zip
,property_data_address_county as county
,eps.state 
,eps.product 
,coalesce(coverage_a,0) as cov_a
,coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0) as tiv
,channel
,written_base
,JSON_EXTRACT_SCALAR(property_data_zillow, '$.zestimate') as zestimate
,JSON_EXTRACT_SCALAR(property_data_zillow, '$.url') as zillow_url
,property_data_zillow
from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, policy_number from dw_prod.dim_policies) dp USING(policy_id)
left join dw_prod.fct_premium_updates fpu on eps.latest_policy_update_id = fpu.policy_update_id
left join (select policy_id, policy_number, channel from dw_prod.dim_quotes) dq on CONCAT(left(eps.policy_number,length(eps.policy_number)-2),'00') = dq.policy_number
left join dw_prod.map_expense_loads as exp ON eps.state=exp.state and eps.product=exp.product and eps.carrier = exp.carrier
where date_snapshot = '2021-01-31'
-- and date_policy_effective >= '2020-07-01'
and eps.carrier <> 'canopius'
)
-- select count(*) from pol
select policy_id, zillow_url from pol
where property_data_zillow is not null