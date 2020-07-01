with snapshot as (
select policy_id
,renewal_number
, case when state = 'tx' and calculated_fields_cat_risk_score = 'referral' then 'referral' 
        when calculated_fields_non_cat_risk_class is null then 'not_applicable' 
        else calculated_fields_non_cat_risk_class end as uw_action 
from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-06-30'
)
, premium as (
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
where date_knowledge = '2020-07-01'
and date_report_period_start >= '2019-09-01'
and carrier <> 'Canopius'
-- and product <> 'HO5'
group by 1,2,3,4,5,6,7,8,9,10
)
, claims_supp as (
select cd.*
,org_id
,renewal_number
,uw_action
, case when peril = 'equipment_breakdown' or peril = 'service_line' then 'Y'
      else 'N' end as EBSL
, case when peril = 'wind' or peril = 'hail' then 'Y'
        when cat_code is not null then 'Y'
        else 'N' end as CAT
from dw_prod_extracts.ext_claims_inception_to_date cd
left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on cd.policy_id = dp.policy_id
left join snapshot eps 
    on cd.policy_id = eps.policy_id
  WHERE date_knowledge = '2020-06-29'
  and carrier <> 'Canopius'
)
, claims as (
select
policy_id
,state
,carrier
,product
,date_trunc(date_of_loss, MONTH) as accident_month
,date_trunc(date_effective, MONTH) as inception_month
,reinsurance_treaty
,org_id as organization_id
,uw_action
,case when renewal_number = 0 then "New" else "Renewal" end as tenure
,sum(total_incurred) as total_incurred
,sum(case when CAT = 'N' then total_incurred else 0 end) as non_cat_incurred
,sum(case when CAT = 'Y' then total_incurred else 0 end) as cat_incurred
,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as total_claim_count_x_cnp
,sum(case when claim_closed_no_total_payment is true and CAT = 'N' then 0 else 1 end) as non_cat_claim_count_x_cnp
,sum(case when claim_closed_no_total_payment is true and CAT = 'Y' then 0 else 1 end) as cat_claim_count_x_cnp
,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then 100000 else total_incurred end) as capped_non_cat_incurred
,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then total_incurred - 100000 else 0 end) as excess_non_cat_incurred
from claims_supp
where ebsl = 'N'
group by 1,2,3,4,5,6,7,8,9,10
)
select p.*
,coalesce(total_incurred,0) as total_incurred
,coalesce(non_cat_incurred,0) as non_cat_incurred
,coalesce(cat_incurred,0) as cat_incurred
,coalesce(total_claim_count_x_cnp,0) as total_claim_count
,coalesce(non_cat_claim_count_x_cnp,0) as non_cat_claim_count
,coalesce(cat_claim_count_x_cnp,0) as cat_claim_count
,coalesce(capped_non_cat_incurred,0) as capped_non_cat_incurred
,coalesce(excess_non_cat_incurred,0) as excess_non_cat_incurred
,DATE_DIFF(p.inception_month, p.accident_month, MONTH)
from premium p 
left join claims c
on 1=1
and p.policy_id = c.policy_id
and p.state = c.state
and p.carrier = c.carrier
and p.product = c.product
and p.accident_month = c.accident_month
and p.inception_month = c.inception_month
and p.accounting_treaty = c.reinsurance_treaty
and p.organization_id = c.organization_id
and p.uw_action = c.uw_action
and p.tenure = c.tenure