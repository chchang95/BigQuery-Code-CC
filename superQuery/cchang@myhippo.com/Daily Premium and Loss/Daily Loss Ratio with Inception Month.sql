with policy_info as (
select eps.policy_id
, date_trunc(date_policy_effective, MONTH) as policy_inception_month
, renewal_number
, org_id
from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on eps.policy_id = dp.policy_id
where date_snapshot = @date_snapshot
)
, premium as (
select state
,carrier
,product
,date_report_period_start as accident_month
,reinsurance_treaty_accounting as accounting_treaty
,org_id as organization_id
,case when dp.renewal_number = 0 then "New" else "Renewal" end as tenure
,policy_inception_month
,sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl
,sum(written_exposure) as written_exposure
,sum(earned_exposure) as earned_exposure
from dw_prod_extracts.ext_today_knowledge_policy_monthly_premiums mon
left join policy_info dp on mon.policy_id = dp.policy_id
where date_knowledge = @today_date
and carrier <> 'Canopius'
group by 1,2,3,4,5,6,7,8
)
, claims_supp as (
select * 
, case when peril = 'equipment_breakdown' or peril = 'service_line' then 'Y'
      else 'N' end as EBSL
, case when peril = 'wind' or peril = 'hail' then 'Y'
        when cat_code is not null then 'Y'
        else 'N' end as CAT
from dw_prod_extracts.ext_claims_inception_to_date cd
left join policy_info dp on cd.policy_id = dp.policy_id
  WHERE date_knowledge = @date_snapshot
  and carrier <> 'Canopius'
)
, claims as (
select
state
,carrier
,product
,date_trunc(date_of_loss, MONTH) as accident_month
,reinsurance_treaty
,org_id as organization_id
,case when renewal_number = 0 then "New" else "Renewal" end as tenure
,policy_inception_month
,sum(total_incurred) as total_incurred
,sum(case when CAT = 'N' then total_incurred else 0 end) as non_cat_incurred
,sum(case when CAT = 'Y' then total_incurred else 0 end) as cat_incurred
,sum(case when claim_closed_no_total_payment is false then 1 else 0 end) as total_claim_count_x_cnp
,sum(case when claim_closed_no_total_payment is false and CAT = 'N' then 1 else 0 end) as non_cat_claim_count_x_cnp
,sum(case when claim_closed_no_total_payment is false and CAT = 'Y' then 1 else 0 end) as cat_claim_count_x_cnp
,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then 100000 else total_incurred end) as capped_non_cat_incurred
,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then total_incurred - 100000 else 0 end) as excess_non_cat_incurred
from claims_supp
where ebsl = 'N'
group by 1,2,3,4,5,6,7,8
) 
, combined as (
select p.*
,coalesce(total_incurred,0) as total_incurred
,coalesce(non_cat_incurred,0) as non_cat_incurred
,coalesce(cat_incurred,0) as cat_incurred
,coalesce(total_claim_count_x_cnp,0) as total_claim_count
,coalesce(non_cat_claim_count_x_cnp,0) as non_cat_claim_count
,coalesce(cat_claim_count_x_cnp,0) as cat_claim_count
,coalesce(capped_non_cat_incurred,0) as capped_non_cat_incurred
,coalesce(excess_non_cat_incurred,0) as excess_non_cat_incurred
from premium p 
left join claims c
on p.state = c.state
and p.carrier = c.carrier
and p.product = c.product
and p.accident_month = c.accident_month
and p.accounting_treaty = c.reinsurance_treaty
and p.organization_id = c.organization_id
and p.tenure = c.tenure
and p.policy_inception_month = c.policy_inception_month
)
, aggregated as (
select 
state, accounting_treaty, accident_month, tenure, policy_inception_month
-- accounting_treaty
, sum(written_prem_x_ebsl) as written_prem, sum(earned_prem_x_ebsl) as earned_prem
, sum(capped_non_cat_incurred) as capped_non_cat_incurred
, sum(excess_non_cat_incurred) as excess_non_cat_incurred
, sum(cat_incurred) as cat_incurred
, sum(total_incurred) as total_incurred
-- , round(sum(capped_non_cat_incurred) / sum(earned_prem_x_ebsl),3) as capped_NC
-- , round(sum(excess_non_cat_incurred) / sum(earned_prem_x_ebsl),3) as excess_NC
-- , round(sum(cat_incurred) / sum(earned_prem_x_ebsl),3) as cat
-- , round(sum(total_incurred) / sum(earned_prem_x_ebsl),3) as total_incurred
from combined
where 1=1
and accident_month >= '2019-09-01'
-- and accounting_treaty = 'Spkr20_Classic'
group by 1,2,3,4,5
order by 1,2,3
)
select * from aggregated