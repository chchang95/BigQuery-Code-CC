with loss as (
SELECT
    policy_id,
    mon.month_knowledge,
    month_of_loss,
    reinsurance_treaty,
    sum(total_calculated_net_paid_delta_this_month) as incremental_paid,
    sum(total_net_reserves_delta_this_month) as incremental_reserves,
    sum(total_incurred_delta_this_month) as incremental_incurred,
    sum(total_incurred_inception_to_date) as cumulative_incurred
  FROM
    dw_prod_extracts.ext_claim_monthly mon
    left join (select claim_number, reinsurance_treaty from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = '2020-10-31') USING(claim_number)
  where is_ebsl is false
  and carrier <> 'canopius'
  group by 1,2,3,4
 )
, premium as (
    select 
        epud.policy_id
        , date_calendar_month_accounting_basis as date_accounting_start
        , reinsurance_treaty_property_accounting
        ,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl
        ,sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
        ,sum(written_exposure) as written_exposure
        ,sum(earned_exposure) as earned_exposure
        ,sum(written_policy_fee) as written_policy_fee
        ,sum(earned_policy_fee) as earned_policy_fee
from dw_prod_extracts.ext_policy_monthly_premiums epud
    left join (select policy_id, policy_number, case when organization_id is null then 0 else organization_id end as org_id, channel from dw_prod.dim_policies) dp on epud.policy_id = dp.policy_id
    left join (select policy_id, calculated_fields_non_cat_risk_class, calculated_fields_cat_risk_class from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2020-10-31') eps on eps.policy_id = epud.policy_id
        where date_knowledge = '2020-10-31'
        and carrier <> 'canopius'
group by 1,2,3
)
, aggregated as (
    select 
coalesce(month_knowledge,date_accounting_start) as calendar_month,
coalesce(month_of_loss,date_accounting_start) as accident_month,
coalesce(p.policy_id, l.policy_id) as policy_id,
coalesce(p.reinsurance_treaty_property_accounting, l.reinsurance_treaty) as reinsurance_treaty
,sum(coalesce(cumulative_incurred,0)) as cumulative_incurred
,sum(coalesce(incremental_incurred,0)) as incremental_incurred
,sum(coalesce(written_prem_x_ebsl,0)) as written_prem_x_ebsl
,sum(coalesce(earned_prem_x_ebsl,0)) as earned_prem_x_ebsl_inc_policy_fees
,sum(coalesce(written_exposure,0)) as written_exposure
,sum(coalesce(earned_exposure,0)) as earned_exposure
,sum(coalesce(written_policy_fee,0)) as written_policy_fee
,sum(coalesce(earned_policy_fee,0)) as earned_policy_fee
from premium p
full join loss l ON
p.date_accounting_start = l.month_knowledge AND
p.date_accounting_start = l.month_of_loss AND
p.policy_id = l.policy_id AND
p.reinsurance_treaty_property_accounting = l.reinsurance_treaty
group by 1,2,3,4
)
select calendar_month, accident_month, carrier, state, product, channel
, case when renewal_number > 0 then 'renewal' else 'new' end as tenure
,sum(cumulative_incurred) as cumulative_incurred
,sum(incremental_incurred) as incremental_incurred
,sum(written_prem_x_ebsl) as written_prem_x_ebsl
,sum(earned_prem_x_ebsl_inc_policy_fees) as earned_prem_x_ebsl_inc_policy_fees
from aggregated a
left join (select * from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2020-10-31') using(policy_id) 
left join (select policy_id, channel from dw_prod.dim_policies) using(policy_id)
group by 1,2,3,4,5,6,7