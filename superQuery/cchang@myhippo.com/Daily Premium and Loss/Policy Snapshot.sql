with claims_supp as (
select * 
, case when peril = 'equipment_breakdown' or peril = 'service_line' then 'Y'
      else 'N' end as EBSL
, case when peril = 'wind' or peril = 'hail' then 'Y'
        when cat_code is not null then 'Y'
        else 'N' end as CAT
from dw_prod_extracts.ext_claims_inception_to_date cd
  WHERE date_knowledge = '2020-07-31'
  and carrier <> 'Canopius'
)
, claims as (
select
policy_id
,date_trunc(date_of_loss, MONTH) as accident_month
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
group by 1,2
) 
select 
eps.policy_id, eps.policy_number
, date_trunc(date_policy_effective, MONTH) as policy_inception_month
,carrier
,state
,product
,status 
,org_id as organization_id
,property_data_address_zip as zip
,property_data_address_county as county
,case when renewal_number = 0 then "New" else "Renewal" end as tenure
, case when state = 'tx' and calculated_fields_cat_risk_score = 'referral' then 'referral' 
        when calculated_fields_non_cat_risk_class is null then 'not_applicable' 
        else calculated_fields_non_cat_risk_class end as uw_action 
, calculated_fields_non_cat_risk_score as non_cat_risk_score
,written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line as written_prem_x_ebsl
,earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line as earned_prem_x_ebsl
,written_exposure
,earned_exposure
,coalesce(total_incurred,0) as total_incurred
,coalesce(non_cat_incurred,0) as non_cat_incurred
,coalesce(cat_incurred,0) as cat_incurred
,coalesce(total_claim_count_x_cnp,0) as total_claim_count_x_cnp
,coalesce(non_cat_claim_count_x_cnp,0) as non_cat_claim_count_x_cnp
,coalesce(cat_claim_count_x_cnp,0) as cat_claim_count_x_cnp
,coalesce(capped_non_cat_incurred,0) as capped_non_cat_incurred
,coalesce(excess_non_cat_incurred,0) as excess_non_cat_incurred
from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on eps.policy_id = dp.policy_id
left join claims c on eps.policy_id = c.policy_id
where date_snapshot = '2020-07-31'
and carrier <> 'Canopius'
and product <> 'HO5'
and state = 'CA'