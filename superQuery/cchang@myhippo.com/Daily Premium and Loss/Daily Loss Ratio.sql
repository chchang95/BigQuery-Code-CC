select state
,carrier
,product
,date_report_period_start as accident_month
,reinsurance_treaty_property_accounting as accounting_treaty
,org_id as organization_id
,date_trunc(date_effective, MONTH) as policy_effective_month
,case when renewal_number = 0 then "New" else "Renewal" end as tenure
,date_trunc(ph.date_first_effective, MONTH) as original_effective_month
,sum(written_base + written_total_optionals  - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl_x_pol_fee
,sum(earned_base + earned_total_optionals - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl_x_pol_fee
,sum(written_exposure) as written_exposure
,sum(earned_exposure) as earned_exposure
from dw_staging_extracts.ext_today_knowledge_policy_monthly_premiums mon
left join (select policy_id, policy_number, policy_group_number from dw_prod.dim_policies) dep on dep.policy_id = mon.policy_id
left join dw_prod.dim_policy_groups ph on dep.policy_group_number = ph.policy_group_number
left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
where date_knowledge = '2020-10-21'
and carrier <> 'canopius'
group by 1,2,3,4,5,6,7,8,9