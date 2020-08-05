with x as(
select
policy_id
,state
,carrier
,product
,date_trunc(date_of_loss, MONTH) as accident_month
,reinsurance_treaty
,org_id as organization_id
,case when renewal_number = 0 then "New" else "Renewal" end as tenure
,policy_inception_month
,uw_action
,zip
,calculated_fields_non_cat_risk_score
,sum(total_incurred) as total_incurred
from dw_prod_extracts.ext_claims_inception_to_date
where 
1=1
and state = 'CA'
and carrier = 'Topa'
group by 1,2,3,4,5,6,7,8,9,10,11,12)
select sum(total_incurred) from x