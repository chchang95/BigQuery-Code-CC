with premium as (
    select 
        epud.policy_id
        ,lower(state) as state
        , lower(carrier) as carrier
        , lower(product) as product
        , extract(year from greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start)) as calendar_year
        , greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start) as date_accounting_start
        , date_sub(date_add(greatest(date_trunc(date_customer_update_made, MONTH), date_trunc(date_update_effective, MONTH), date_report_period_start), INTERVAL 1 MONTH), INTERVAL 1 DAY) as date_accounting_end
        , case when reinsurance_treaty = 'Spkr19_GAP' and date_report_period_start <= '2019-12-31' then 'Spkr19_GAP'
            when reinsurance_treaty = 'Spkr19_GAP' and date_report_period_start >= '2020-01-01' then 'Spkr20_Classic'
            else reinsurance_treaty end as accounting_treaty
        ,org_id as organization_id
        ,sum(earned_base + earned_total_optionals + earned_policy_fee - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_inc_policy_fee_ex_EBSL
        ,sum(written_base + written_total_optionals + written_policy_fee - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_inc_policy_fee_ex_EBSL
        ,sum(earned_policy_fee) as earned_policy_fee
        ,sum(written_policy_fee) as written_policy_fee
        ,sum(earned_optionals_equipment_breakdown + earned_optionals_service_line) as earned_EBSL
        ,sum(written_optionals_equipment_breakdown + written_optionals_service_line) as written_EBSL
        ,sum(written_exposure) as written_exposure
        ,sum(earned_exposure) as earned_exposure
from dw_prod_extracts.ext_policy_update_monthly_premiums epud
    left join (select policy_id, policy_number, reinsurance_treaty, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on epud.policy_id = dp.policy_id
        where date_knowledge = '2020-06-30'
        and carrier <> 'Canopius'
        -- and product <> 'HO5'
group by 1,2,3,4,5,6,7,8,9
)
    select 
        state
        ,carrier
        ,product
        ,calendar_year
        ,date_accounting_start
        ,date_accounting_end
        ,accounting_treaty
        ,organization_id
        ,case when renewal_number = 0 then "New" else "Renewal" end as tenure
        ,sum(written_prem_inc_policy_fee_ex_EBSL) as written_prem_inc_policy_fee_ex_EBSL
        ,sum(earned_prem_inc_policy_fee_ex_EBSL) as earned_prem_inc_policy_fee_ex_EBSL
        ,sum(written_policy_fee) as written_policy_fee
        ,sum(earned_policy_fee) as earned_policy_fee
        ,sum(written_EBSL) as written_EBSL
        ,sum(earned_EBSL) as earned_EBSL
        ,sum(written_exposure) as written_exposure
        ,sum(earned_exposure) as earned_exposure
from premium p
left join (select policy_id, date_snapshot, coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0) as TIV, renewal_number
      from dw_prod_extracts.ext_policy_snapshots) eps on p.policy_id = eps.policy_id and p.date_accounting_end = eps.date_snapshot
group by 1,2,3,4,5,6,7,8,9