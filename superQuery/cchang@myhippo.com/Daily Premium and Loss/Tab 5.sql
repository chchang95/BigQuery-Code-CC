select eps.policy_id
, date_trunc(date_policy_effective, MONTH) as policy_inception_month
, renewal_number
, org_id
, property_data_address_zip as zip
, case when state = 'tx' and calculated_fields_cat_risk_score = 'referral' then 'referral' 
        when calculated_fields_non_cat_risk_class is null then 'not_applicable' 
        else calculated_fields_non_cat_risk_class end as uw_action 
from dw_prod_extracts.ext_policy_snapshots eps
left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on eps.policy_id = dp.policy_id
where date_snapshot = @date_snapshot
and calculated_fields_non_cat_risk_class is null
and carrier <> 'Canopius'
and date_policy_effective >= '2020-05-01'
and product <> 'HO5'
and renewal_number = 0