with loss as (
SELECT
    mon.month_knowledge,
    mon.carrier,
    mon.state,
    mon.product,
    month_of_loss,
    maturity,
    sum(total_calculated_net_paid_delta_this_month) as incremental_paid,
    sum(total_net_reserves_delta_this_month) as incremental_reserves,
    sum(total_incurred_delta_this_month) as incremental_incurred,
    sum(total_incurred_inception_to_date) as cumulative_incurred
  FROM
    dw_prod_extracts.ext_claim_monthly mon
    left join (select claim_number, reinsurance_treaty from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = '2020-10-31') USING(claim_number)
  where is_ebsl is false
  and carrier <> 'canopius'
  group by 1,2,3,4,5,6
 )
, premium as (
    select 
        epud.policy_id
        , lower(state) as state
        , lower(carrier) as carrier
        , lower(product) as product
        , extract(year from date_calendar_month_accounting_basis) as calendar_year
        , date_calendar_month_accounting_basis as date_accounting_start
        , date_sub(date_add(date_calendar_month_accounting_basis, INTERVAL 1 MONTH), INTERVAL 1 DAY) as date_accounting_end
        , reinsurance_treaty_property_accounting
        , org_id as organization_id
        , channel
        , case when renewal_number = 0 then "New" else "Renewal" end as tenure
        , date_trunc(date_effective, MONTH) as term_effective_month
        , case when state = 'tx' and calculated_fields_cat_risk_class = 'referral' then 'cat referral' 
              when calculated_fields_non_cat_risk_class is null or date_effective <= '2020-05-01' then 'not_applicable'
              else calculated_fields_non_cat_risk_class end as rated_uw_action
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
        -- and product <> 'HO5'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
, aggregated as (
    select 
coalesce(month_knowledge,date_accounting_start) as calendar_month,
coalesce(month_of_loss,date_accounting_start) as accident_month,
coalesce(p.carrier, l.carrier) as carrier,
coalesce(p.state, l.state) as state,
coalesce(p.product, l.product) as product
,sum(coalesce(cumulative_incurred,0)) as cumulative_incurred
,sum(coalesce(incremental_incurred,0)) as incremental_incurred
,sum(written_prem_x_ebsl) as written_prem_x_ebsl
,sum(earned_prem_x_ebsl) as earned_prem_x_ebsl_inc_policy_fees
,sum(written_exposure) as written_exposure
,sum(earned_exposure) as earned_exposure
,sum(written_policy_fee) as written_policy_fee
,sum(earned_policy_fee) as earned_policy_fee
from premium p
full join loss l ON
p.date_accounting_start = l.month_knowledge AND
p.date_accounting_start = l.month_of_loss AND
p.carrier = l.carrier AND
p.state = l.state AND
p.product = l.product
group by 1,2,3,4,5
)
select * from aggregated