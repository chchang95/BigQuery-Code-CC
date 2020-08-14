with premium as (
    select 
        epud.policy_id
        ,lower(state) as state
        , lower(carrier) as carrier
        , lower(product) as product
        , extract(year from date_calendar_month_accounting_basis) as calendar_year
        , date_calendar_month_accounting_basis as date_accounting_start
        , date_sub(date_add(date_calendar_month_accounting_basis, INTERVAL 1 MONTH), INTERVAL 1 DAY) as date_accounting_end
        , reinsurance_treaty_accounting
        ,org_id as organization_id
        ,case when renewal_number = 0 then "New" else "Renewal" end as tenure
        ,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl
        ,sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl
        ,sum(written_exposure) as written_exposure
        ,sum(earned_exposure) as earned_exposure
        ,sum(written_policy_fee) as written_policy_fee
        ,sum(earned_policy_fee) as earned_policy_fee
from dw_prod_extracts.ext_policy_monthly_premiums epud
    left join (select policy_id, policy_number, reinsurance_treaty, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on epud.policy_id = dp.policy_id
        where date_knowledge = '2020-07-31'
        and carrier <> 'Canopius'
        -- and product <> 'HO5'
group by 1,2,3,4,5,6,7,8,9,10
)
    select 
        state
        ,carrier
        ,product
        ,calendar_year
        ,date_accounting_start
        ,date_accounting_end
        ,reinsurance_treaty_accounting
        -- ,organization_id
        -- ,tenure
        -- ,sum(written_prem_x_ebsl) as written_prem_x_ebsl
        ,sum(earned_prem_x_ebsl) as earned_prem_x_ebsl_inc_policy_fees
        -- ,sum(written_exposure) as written_exposure
        ,sum(earned_exposure) as earned_exposure
        -- ,sum(written_exposure * TIV) as written_TIV
        ,sum(earned_exposure * TIV) as earned_TIV
        -- ,sum(written_policy_fee) as written_policy_fee
        ,sum(earned_policy_fee) as earned_policy_fee
from premium p
left join (select policy_id, date_snapshot, coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0) as TIV
      from dw_prod_extracts.ext_policy_snapshots) eps on p.policy_id = eps.policy_id and p.date_accounting_end = eps.date_snapshot
group by 1,2,3,4,5,6,7