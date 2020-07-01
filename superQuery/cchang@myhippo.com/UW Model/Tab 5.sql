with snapshot as (
select policy_id
,renewal_number
, case when state = 'tx' and calculated_fields_cat_risk_score = 'referral' then 'referral' 
        when calculated_fields_non_cat_risk_class is null then 'not_applicable' 
        else calculated_fields_non_cat_risk_class end as uw_action 
from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-06-30'
)
, claims_supp as (
select cd.*
,org_id
,renewal_number
,uw_action
,date_trunc(date_of_loss, MONTH) as accident_month
,date_trunc(date_first_notice_of_loss, MONTH) as report_month
,date_trunc(date_effective, MONTH) as inception_month
, case when peril = 'equipment_breakdown' or peril = 'service_line' then 'Y'
      else 'N' end as EBSL
, case when peril = 'wind' or peril = 'hail' then 'Y'
        when cat_code is not null then 'Y'
        else 'N' end as CAT
from dw_prod_extracts.ext_claims_inception_to_date cd
left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on cd.policy_id = dp.policy_id
left join snapshot eps 
    on cd.policy_id = eps.policy_id
  WHERE date_knowledge = '2020-06-30'
  and carrier <> 'Canopius'
) select
-- policy_id
-- ,state
-- ,carrier
-- ,product
-- ,accident_month
-- ,inception_month
-- ,reinsurance_treaty
-- ,org_id as organization_id
-- ,uw_action
-- ,case when renewal_number = 0 then "New" else "Renewal" end as tenure
-- ,
sum(total_incurred) as total_incurred
,sum(case when CAT = 'N' then total_incurred else 0 end) as non_cat_incurred
,sum(case when CAT = 'Y' then total_incurred else 0 end) as cat_incurred
,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as total_claim_count_x_cnp
,sum(case when claim_closed_no_total_payment is false and CAT = 'N' then 1 else 0 end) as non_cat_claim_count_x_cnp
,sum(case when claim_closed_no_total_payment is false and CAT = 'Y' then 1 else 0 end) as cat_claim_count_x_cnp
,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then 100000 else total_incurred end) as capped_non_cat_incurred
,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then total_incurred - 100000 else 0 end) as excess_non_cat_incurred
from claims_supp
where ebsl = 'N'
-- and DATE_DIFF(report_month, inception_month, MONTH) <= 1
-- group by 1,2,3,4,5,6,7,8,9,10