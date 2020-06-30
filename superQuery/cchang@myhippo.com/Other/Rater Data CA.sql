with latest_prem_update as (
select * 
,FIRST_VALUE(policy_update_id) OVER (PARTITION by policy_id ORDER BY timestamp_update_made desc, date_update_effective DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_prem
from dw_prod.fct_premium_updates
left join (select policy_update_id, policy_id, is_premium_bearing_update, timestamp_update_made, date_update_effective from dw_prod.fct_policy_updates) using (policy_update_id)
order by policy_id
)
,latest_update as (
select *
,coalesce(quote_premium_optionals,0) - coalesce(quote_optionals_equipment_breakdown,0) - coalesce(quote_optionals_service_line,0) - coalesce(quote_optionals_scheduled_personal_property,0) as min_optionals_adjustment
,round(quote_by_peril_fire,0) as p1_fire
,round(quote_by_peril_liability,0) as p2_liability
,round(quote_by_peril_theft,0) as p3_theft
,round(quote_by_peril_wind,0) as p4_wind
,round(quote_by_peril_water,0) as p5_water
,round(quote_by_peril_other,0) as p6_other
,round(quote_by_peril_cat_wind,0) as p7_cat_wind
,round(quote_by_peril_cat_fire,0) as p8_cat_fire
,round(quote_by_peril_hurricane,0) as p9_hurricane
,round(quote_by_peril_cat_water,0) as p10_cat_water
,(round(quote_by_peril_theft,0) + round(quote_by_peril_other,0) + round(quote_by_peril_water,0) + round(quote_by_peril_cat_wind,0) + round(quote_by_peril_liability,0) + round(quote_by_peril_fire,0) + round(quote_by_peril_wind,0) + round(quote_by_peril_cat_fire,0) + round(quote_by_peril_hurricane,0) + round(quote_by_peril_cat_water,0)) as sum_perils
from latest_prem_update
where policy_update_id = latest_prem
)
select distinct policy_id, policy_number
,property_data_address_zip
,property_data_address_county
,carrier
,status
,state
,date_policy_effective
,renewal_number as term
,product
,coverage_a
,calculated_fields_age_of_home as age_of_home
,calculated_fields_age_of_roof as age_of_roof
,calculated_fields_age_of_insured as age_of_insured
,calculated_fields_early_quote_days as eq_days
,eps.property_data_roof_type as roof_type
,property_data_smart_home_water_leak_device as water_device
,property_data_smart_home_kit_activated as smart_kit_activate
,eps.quote_rater_version as rater_version
,eps.written_base + eps.written_total_optionals - eps.written_optionals_equipment_breakdown - eps.written_optionals_service_line as written_x_ebsl
,eps.written_optionals_equipment_breakdown as written_eb
,eps.written_optionals_service_line as written_sl
,case when eps.written_optionals_equipment_breakdown > 0 then 'Y' else 'N' end as EB_ind
,case when eps.written_optionals_service_line > 0 then 'Y' else 'N' end as SL_ind
,quote_premium_base as base
,quote_premium_optionals as optionals
,quote_inspection_fee as inspection_fees
,written_inspection_fee as written_inspection_fee
,written_policy_fee as written_policy_fees
,quote_premium_total as total
,coalesce(quote_premium_optionals,0) - coalesce(quote_optionals_equipment_breakdown,0) - coalesce(quote_optionals_service_line,0) - coalesce(quote_optionals_scheduled_personal_property,0) as min_optionals_adjustment
,coverage_a / 1000 as minimum_premium
,p1_fire
,p2_liability
,p3_theft 
,p4_wind 
,p5_water 
,p6_other 
,p7_cat_wind 
,p8_cat_fire 
,p9_hurricane 
,p10_cat_water 
,greatest(greatest(coverage_a / 1000,200) - sum_perils - min_optionals_adjustment - written_policy_fee,0) as minimum_prem_adjustment
-- ,quote_premium_base - greatest(coverage_a / 1000 - sum_perils - min_optionals_adjustment ,0) - sum_perils as expense_load
,0 as expense_load
from dw_staging_extracts.ext_policy_snapshots eps
left join (select policy_id, policy_number from dw_prod.dim_policies) dp USING(policy_id)
left join latest_update lu USING (policy_id)
where eps.date_snapshot = '2020-05-25'
and state = 'CA'
and product <> 'HO5'
and carrier = 'Topa'
-- and status = 'active'
-- and policy_number = 'HTX-0815687-00'
-- and coverage_a is null
LIMIT 30000