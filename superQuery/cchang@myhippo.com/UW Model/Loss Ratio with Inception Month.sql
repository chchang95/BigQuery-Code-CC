select 
mon.policy_id
,state
,carrier
,product
,date_report_period_start as accident_month
,inception_month
,reinsurance_treaty_accounting as accounting_treaty
,org_id as organization_id
,uw_action
,case when mon.renewal_number = 0 then "New" else "Renewal" end as tenure
,sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl
,sum(written_exposure) as written_exposure
,sum(earned_exposure) as earned_exposure
from dw_prod_extracts.ext_today_knowledge_policy_monthly_premiums mon
left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id,date_trunc(date_effective, MONTH) as inception_month from dw_prod.dim_policies) dp 
    on mon.policy_id = dp.policy_id
left join snapshot eps 
    on mon.policy_id = eps.policy_id
where date_knowledge = '2020-06-30'
and date_report_period_start >= '2019-09-01'
and carrier <> 'Canopius'
-- and product <> 'HO5'
group by 1,2,3,4,5,6,7,8,9,10