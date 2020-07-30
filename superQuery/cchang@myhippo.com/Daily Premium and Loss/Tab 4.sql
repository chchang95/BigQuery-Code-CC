select eps.policy_id, eps.policy_number
, date_trunc(date_policy_effective, MONTH) as policy_inception_month
,carrier
,product
,org_id as organization_id
,case when renewal_number = 0 then "New" else "Renewal" end as tenure
, case when state = 'tx' and calculated_fields_cat_risk_score = 'referral' then 'referral' 
        when calculated_fields_non_cat_risk_class is null then 'not_applicable' 
        else calculated_fields_non_cat_risk_class end as uw_action 
,sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl
,sum(written_exposure) as written_exposure
,sum(earned_exposure) as earned_exposure
from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on eps.policy_id = dp.policy_id
where date_snapshot = '2020-06-30'
group by 1,2,3,4,5,6,7,8