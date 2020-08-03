with snapshot as (
select policy_id
,renewal_number
, case when state = 'tx' and calculated_fields_cat_risk_score = 'referral' then 'referral' 
        when calculated_fields_non_cat_risk_class is null then 'not_applicable' 
        else calculated_fields_non_cat_risk_class end as uw_action 
from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-07-31'
)
, claims as (select cd.*
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
  WHERE date_knowledge = '2020-07-31'
  and carrier <> 'Canopius'
)
select 
claim_id
,policy_id
,policy_number
,claim_number
,date_of_loss
,date_first_notice_of_loss as report_date
,peril
,product
,carrier
,date_effective
,date_expires
,uw_action
,renewal_number
,total_incurred
from claims
where EBSL = 'N'
and CAT = 'N'
and date_effective >= '2020-05-01'
and uw_action = 'referral'
and claim_closed_no_total_payment = false
and renewal_number = 0
order by date_effective desc, date_of_loss desc, total_incurred desc