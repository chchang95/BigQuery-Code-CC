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
-- ,coalesce(coverage_b,0) as cov_b
-- ,coalesce(coverage_c,0) as cov_c
-- ,coalesce(coverage_d,0) as cov_d
,coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0) as tiv
,channel
,written_base
,JSON_EXTRACT_SCALAR(property_data_zillow, '$.zestimate') as zestimate
property_data_zillow
-- ,written_total_optionals
-- ,written_optionals_equipment_breakdown
-- ,written_optionals_service_line
-- ,written_sum_perils
-- ,written_policy_fee
-- ,(expense_load_digital) as written_expense_load
-- ,(expense_load_digital*eps.earned_exposure) as earned_expense_load

-- ,((written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line - expense_load_digital) * coalesce(on_level_factor,1)) as on_leveled_written_prem_x_ebsl_x_pol_fees_x_exp_load
-- ,((earned_base + earned_total_optionals - earned_optionals_equipment_breakdown - earned_optionals_service_line - expense_load_digital*earned_exposure) * coalesce(on_level_factor,1)) as on_leveled_earned_prem_x_ebsl_x_pol_fees_x_exp_load
-- ,region_code
-- ,coverage_wind_deductible as wind_deductible

-- ,JSON_EXTRACT_SCALAR(property_data_zillow,'$.zestimate') as zillow_market_value
-- ,property_data_zillow
-- ,calculated_fields_market_value_much_below_rce
-- ,prefilled_rebuilding_cost
-- ,zestim
from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, policy_number from dw_prod.dim_policies) dp USING(policy_id)
left join dw_prod.fct_premium_updates fpu on eps.latest_policy_update_id = fpu.policy_update_id
left join (select policy_id, policy_number, channel from dw_prod.dim_quotes) dq on CONCAT(left(eps.policy_number,length(eps.policy_number)-2),'00') = dq.policy_number
left join dw_prod.map_expense_loads as exp ON eps.state=exp.state and eps.product=exp.product and eps.carrier = exp.carrier
where date_snapshot = '2021-01-31'
-- and date_policy_effective >= '2020-07-01'
and eps.carrier <> 'canopius'
-- and eps.product = 'ho3'
-- and status = 'active'
-- and carrier = 'spinnaker'
-- and state = 'ca'
-- and eps.state = 'tx'
-- and property_data_address_zip = '78332'
-- and calculated_fields_wind_exclusion <> 'true'
-- and date_policy_effective <= '2020-05-31'
)
-- select count(*) from pol
select * from pol
-- where cov_a >= 1100000
-- order by 1 desc
-- where policy_number = 'HAZ-1348250-00'
-- where building_quality is null