with pol as (
select eps.policy_id
,status
-- ,eps.is_status_active
-- ,date_cancellation
,dp.policy_number
-- ,date_policy_effective as effective_date
-- ,date_policy_expires as expiration_date
-- ,property_data_address_street as street
-- ,property_data_address_city as city
,property_data_address_zip as zip
,property_data_address_county as county
-- ,eps.state 
-- ,eps.product 
-- ,calculated_fields_age_of_home as age_of_home
-- ,insurance_score
-- ,coverage_deductible as deductible
-- -- ,property_data_protection_class
-- -- ,prefilled_fireline_score
-- ,written_base + written_total_optionals + written_policy_fee as written_total_premium_inc_pol_fees
-- ,prefilled_rebuilding_cost as prefilled_rebuilding_cost
-- ,property_data_rebuilding_cost as property_data_rebuilding_cost
-- ,property_data_building_quality as building_quality
-- ,case when property_data_building_quality = 'standard' then 'E'
-- when property_data_building_quality = 'premium' then 'A'
-- when property_data_building_quality = 'luxury' then 'P'
-- else 'Error' end as mapping_building_quality
-- ,property_data_square_footage as square_foot
-- ,coalesce(coverage_a,0) as cov_a
-- ,coalesce(coverage_b,0) as cov_b
-- ,coalesce(coverage_c,0) as cov_c
-- ,coalesce(coverage_d,0) as cov_d
-- ,coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0) as tiv
-- ,channel
-- ,dq.organization_id
-- ,o.name as org_name
-- ,written_base
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
,reinsurance_treaty_property

from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, policy_number from dw_prod.dim_policies) dp USING(policy_id)
left join dw_prod.fct_premium_updates fpu on eps.latest_policy_update_id = fpu.policy_update_id
left join (select policy_id, policy_number, channel, organization_id from dw_prod.dim_quotes) dq on CONCAT(left(eps.policy_number,length(eps.policy_number)-2),'00') = dq.policy_number
left join dw_prod.map_expense_loads as exp ON eps.state=exp.state and eps.product=exp.product and eps.carrier = exp.carrier
LEFT JOIN dw_prod.dim_organizations o on dq.organization_id = o.organization_id
where date_snapshot = '2021-03-31'
-- and date_policy_effective >= '2020-07-01'
and eps.carrier <> 'canopius'
and state = 'ca'
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

