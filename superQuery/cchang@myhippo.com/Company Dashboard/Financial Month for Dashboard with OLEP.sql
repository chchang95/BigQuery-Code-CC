with premium as (
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
        ,sum((written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line) * coalesce(on_level_factor,1) + written_policy_fee) as on_leveled_written_prem_x_ebsl
        ,sum((earned_base + earned_total_optionals - earned_optionals_equipment_breakdown - earned_optionals_service_line) * coalesce(on_level_factor,1) + earned_policy_fee) as on_leveled_earned_prem_x_ebsl
from dw_prod_extracts.ext_policy_monthly_premiums epud
    left join (select policy_id, policy_number, case when organization_id is null then 0 else organization_id end as org_id, channel from dw_prod.dim_policies) dp on epud.policy_id = dp.policy_id
    left join (select policy_id, calculated_fields_non_cat_risk_class, calculated_fields_cat_risk_class from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2020-11-30') eps on eps.policy_id = epud.policy_id
    left join (select policy_id, on_level_factor from dw_staging_extracts.ext_policy_snapshots where date_snapshot = '2020-11-30') seps on seps.policy_id = epud.policy_id
        where date_knowledge = '2020-11-30'
        and carrier <> 'canopius'
        -- and product <> 'HO5'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
, aggregated as (
    select 
        state
        ,carrier
        ,product
        ,calendar_year
        ,date_accounting_start
        ,date_accounting_end
        ,reinsurance_treaty_property_accounting
        -- ,organization_id
        ,channel
        ,tenure
        ,term_effective_month
        ,rated_uw_action
        ,sum(written_prem_x_ebsl) as written_prem_x_ebsl
        ,sum(earned_prem_x_ebsl) as earned_prem_x_ebsl_inc_policy_fees
        ,sum(written_exposure) as written_exposure
        ,sum(earned_exposure) as earned_exposure
        ,sum(written_exposure * TIV) as written_TIV
        ,sum(earned_exposure * TIV) as earned_TIV
        ,sum(written_policy_fee) as written_policy_fee
        ,sum(earned_policy_fee) as earned_policy_fee
        ,sum(on_leveled_written_prem_x_ebsl) as on_leveled_written_prem_x_ebsl
        ,sum(on_leveled_earned_prem_x_ebsl) as on_leveled_earned_prem_x_ebsl
from premium p
left join (select policy_id, date_snapshot, coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0) as TIV
      from dw_prod_extracts.ext_policy_snapshots) eps on p.policy_id = eps.policy_id and p.date_accounting_end = eps.date_snapshot
group by 1,2,3,4,5,6,7,8,9,10,11
)
, summary as (
    select 
        reinsurance_treaty_property_accounting,
        sum(written_prem_x_ebsl) as written_prem_x_ebsl
        ,sum(earned_prem_x_ebsl) as earned_prem_x_ebsl_inc_policy_fees
        ,sum(written_exposure) as written_exposure
        ,sum(earned_exposure) as earned_exposure
        ,sum(written_exposure * TIV) as written_TIV
        ,sum(earned_exposure * TIV) as earned_TIV
        ,sum(written_policy_fee) as written_policy_fee
        ,sum(earned_policy_fee) as earned_policy_fee
from premium p
left join (select policy_id, date_snapshot, coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0) as TIV
      from dw_prod_extracts.ext_policy_snapshots) eps on p.policy_id = eps.policy_id and p.date_accounting_end = eps.date_snapshot
group by 1
)
select * from aggregated