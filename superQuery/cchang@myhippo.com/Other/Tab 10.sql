select distinct policy_id
, policy_number
,property_data_address_zip
,property_data_address_county
,carrier
,status
,state
,date_policy_effective
,renewal_number as term
,product
,coverage_a
,eps.quote_rater_version as rater_version
,eps.written_base + eps.written_total_optionals - eps.written_optionals_equipment_breakdown - eps.written_optionals_service_line as written_x_ebsl
-- ,quote_premium_base - greatest(coverage_a / 1000 - sum_perils - min_optionals_adjustment ,0) - sum_perils as expense_load
,0 as expense_load
from dw_staging_extracts.ext_policy_snapshots eps
where eps.date_snapshot = '2020-05-31'
and state = 'TX'
and product <> 'HO5'