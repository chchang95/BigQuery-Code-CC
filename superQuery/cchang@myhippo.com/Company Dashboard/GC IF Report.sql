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
-- ,fpu.quote_premium_total 
-- ,fpu.quote_premium_base
-- ,fpu.quote_premium_fees
-- ,fpu.quote_premium_optionals
-- ,fpu.quote_optionals_equipment_breakdown
-- ,fpu.quote_optionals_service_line
,written_base + written_total_optionals + written_policy_fee as written_total_w_pol_fees_w_ebsl
,written_base
,written_policy_fee
,written_total_optionals
,written_optionals_equipment_breakdown
,written_optionals_service_line
,earned_base + earned_total_optionals + earned_policy_fee as earned_total_w_pol_fees_w_ebsl
,earned_policy_fee
,earned_optionals_equipment_breakdown
,earned_optionals_service_line
,property_data_roof_type as Roof_Type
,property_data_roof_shape as Roof_Shape
,property_data_year_built as Year_Built
,property_data_residence_type as Residence_Type
,property_data_square_footage as Square_Footage
,property_data_foundation_type as Foundation_Type
,property_data_year_roof_built as Year_Roof_Built
,property_data_protection_class as Protection_Class
,property_data_construction_type as Construction_Type
,property_data_number_of_stories as Number_Of_Stories
,property_data_hail_resistant_roof as Hail_Resistant_Roof
,property_data_number_of_family_units as Number_Of_Family_Units
,prefilled_basement_finished_percent as Basement_Finished_Percent
,property_data_other_wind_loss_prevention as Other_Wind_Loss_Prevention
,calculated_fields_age_of_roof as Age_Of_Roof
,calculated_fields_wind_exclusion as Wind_Exclusion
,coverage_deductible as Deductible
,coverage_hurricane_deductible as Hurricane_Deductible
,coverage_wind_deductible as Wind_Deductible
,coverage_a as cov_a
,coverage_b as cov_b
,coverage_c as cov_c
,coverage_d as cov_d
,property_data_geocoordinates_latitude
,property_data_geocoordinates_longitude
-- , channel
-- , org_id
-- ,do.name as org_name
-- ,property_data_territory_rater_code
,reinsurance_treaty_property
from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, policy_number from dw_prod.dim_policies) dp USING(policy_id)
left join dw_prod.fct_premium_updates fpu on eps.latest_policy_update_id = fpu.policy_update_id
left join (select policy_id, case when channel is null then 'Online' else channel end as channel, coalesce(organization_id,0) as org_id from dw_prod.dim_policies) using(policy_id)
left join dw_prod.dim_organizations do on org_id = do.organization_id
where date_snapshot = '2021-05-31'
and carrier <> 'canopius'
-- and reinsurance_treaty_property = 'spkr21_core'
-- and product <> 'ho5'
and status = 'active'
and carrier = 'spinnaker'
-- and state = 'ca'
-- and state = 'tx'
-- and property_data_address_zip = '78332'
-- and calculated_fields_wind_exclusion <> 'true'
-- and date_policy_effective <= '2020-05-31'
