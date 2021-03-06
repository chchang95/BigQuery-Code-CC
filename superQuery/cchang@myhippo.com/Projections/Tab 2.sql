with claims_supp as (
select mon.*
, case when cc.cat_ind is true then 'Y'
    when cc.cat_ind is false then 'N'
    when peril = 'wind' or peril = 'hail' then 'Y'
    when cat_code is not null then 'Y'
        else 'N' end as CAT
, case when large_loss is true then 'Y' else 'N' end as large_loss
from dw_prod_extracts.ext_claim_monthly mon
left join dbt_cchin.cat_coding_20201130 cc on mon.claim_number = cc.claim_number
left join dbt_cchin.LL_listing_20201130 LL on mon.claim_number = LL.claim_number
where carrier <> 'canopius'
)
, loss as (
SELECT
    policy_id,
    mon.month_knowledge,
    month_of_loss,
    reinsurance_treaty,
    sum(case when CAT = 'Y' then total_incurred_delta_this_month else 0 end) as CAT_incurred_loss_and_alae_incremental,
    sum(case when CAT = 'N' then total_incurred_delta_this_month else 0 end) as NonCAT_incurred_loss_and_alae_incremental,
    sum(case when CAT = 'Y' then expense_calculated_incurred_delta_this_month else 0 end) as CAT_incurred_alae_incremental,
    sum(case when CAT = 'N' then expense_calculated_incurred_delta_this_month else 0 end) as NonCAT_incurred_alae_incremental,
    sum(case when CAT = 'Y' then loss_calculated_incurred_delta_this_month else 0 end) as CAT_incurred_loss_incremental,
    sum(case when CAT = 'N' then loss_calculated_incurred_delta_this_month else 0 end) as NonCAT_incurred_loss_incremental,
    sum(case when CAT = 'N' and large_loss = 'Y' then total_incurred_delta_this_month else 0 end) as NonCat_Large_LLAE_incremental,
    sum(case when CAT = 'N' and large_loss = 'N' then total_incurred_delta_this_month else 0 end) as NonCat_Small_LLAE_incremental
  FROM
    claims_supp mon
    left join (select claim_number, reinsurance_treaty from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = '2020-11-30') USING(claim_number)
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
    left join (select policy_id, calculated_fields_non_cat_risk_class, calculated_fields_cat_risk_class from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2020-11-30') eps on eps.policy_id = epud.policy_id
        where date_knowledge = '2020-11-30'
        and carrier <> 'canopius'
group by 1,2,3
)
, aggregated as (
    select 
coalesce(month_knowledge,date_accounting_start) as calendar_month,
coalesce(month_of_loss,date_accounting_start) as accident_month,
coalesce(p.policy_id, l.policy_id) as policy_id,
coalesce(p.reinsurance_treaty_property_accounting, l.reinsurance_treaty) as reinsurance_treaty
,sum(coalesce(CAT_incurred_loss_and_alae_incremental,0)) as CAT_incurred_loss_and_alae_incremental
,sum(coalesce(NonCAT_incurred_loss_and_alae_incremental,0)) as NonCAT_incurred_loss_and_alae_incremental

,sum(coalesce(CAT_incurred_alae_incremental,0)) as CAT_incurred_alae_incremental
,sum(coalesce(NonCAT_incurred_alae_incremental,0)) as NonCAT_incurred_alae_incremental

,sum(coalesce(CAT_incurred_loss_incremental,0)) as CAT_incurred_loss_incremental
,sum(coalesce(NonCAT_incurred_loss_incremental,0)) as NonCAT_incurred_loss_incremental

,sum(coalesce(NonCat_Large_LLAE_incremental,0)) as NonCat_Large_LLAE_incremental
,sum(coalesce(NonCat_Small_LLAE_incremental,0)) as NonCat_Small_LLAE_incremental

,sum(coalesce(written_prem_x_ebsl,0)) as written_prem_x_ebsl_inc_policy_fees
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
select calendar_month, accident_month, carrier, state, product, channel, reinsurance_treaty
, case when renewal_number > 0 then 'RB' else 'NB' end as tenure
,sum(coalesce(CAT_incurred_loss_and_alae_incremental,0)) as CAT_incurred_loss_and_alae_incremental
,sum(coalesce(NonCAT_incurred_loss_and_alae_incremental,0)) as NonCAT_incurred_loss_and_alae_incremental

,sum(coalesce(CAT_incurred_alae_incremental,0)) as CAT_incurred_alae_incremental
,sum(coalesce(NonCAT_incurred_alae_incremental,0)) as NonCAT_incurred_alae_incremental

,sum(coalesce(CAT_incurred_loss_incremental,0)) as CAT_incurred_loss_incremental
,sum(coalesce(NonCAT_incurred_loss_incremental,0)) as NonCAT_incurred_loss_incremental

,sum(coalesce(NonCat_Large_LLAE_incremental,0)) as NonCat_Large_LLAE_incremental
,sum(coalesce(NonCat_Small_LLAE_incremental,0)) as NonCat_Small_LLAE_incremental

,sum(written_prem_x_ebsl_inc_policy_fees) as written_prem_x_ebsl_inc_policy_fees
,sum(earned_prem_x_ebsl_inc_policy_fees) as earned_prem_x_ebsl_inc_policy_fees
,sum(coalesce(a.written_policy_fee,0)) as written_policy_fee
,sum(coalesce(a.earned_policy_fee,0)) as earned_policy_fee
from aggregated a
left join (select * from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2020-11-30') using(policy_id) 
left join (select policy_id, case when channel is null then 'Online' else channel end as channel from dw_prod.dim_policies) using(policy_id)
where calendar_month is not null
group by 1,2,3,4,5,6,7,8