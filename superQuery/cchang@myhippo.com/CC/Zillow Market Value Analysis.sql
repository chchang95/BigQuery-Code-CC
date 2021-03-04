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
select * from pol
where property_data_zillow is not null



-- select cast(id as string)   as policy_number
-- , policy_info
-- ,data
-- from postgres_public.policies a
-- where 1 = 1
--       and bound is true
--       and status not in ('pending_active', 'pending_bind')
-- --       and effective_date::date <= '2020-04-31'::date
-- and cast(initial_quote_date as date) >= '2019-10-20'
-- --       and state = 'tx'
-- and carrier <> 'canopius'
-- and product not in ('ho5')
-- and json_extract_scalar(coalesce(policy_info,transaction),'$.quote.premium.total') is not null
-- and state is not null
-- and initial_quote_date is not null
-- and effective_date is not null
--   LIMIT 100000