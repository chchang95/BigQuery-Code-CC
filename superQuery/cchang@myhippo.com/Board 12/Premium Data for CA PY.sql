with premium as (
    select 
        epud.policy_id
        ,lower(epud.state) as state
        ,lower(epud.carrier) as carrier
        ,lower(epud.product) as product
        ,property_data_address_zip as zip
        , property_data_address_county as county
        ,extract(year from date_calendar_month_accounting_basis) as calendar_year
        ,date_calendar_month_accounting_basis as date_accounting_start
        ,date_sub(date_add(date_calendar_month_accounting_basis, INTERVAL 1 MONTH), INTERVAL 1 DAY) as date_accounting_end
        ,reinsurance_treaty_property_accounting
        ,case when renewal_number = 0 then "New" else "Renewal" end as tenure
        , date_effective as term_effective
        , last_day(date_trunc(date_first_effective, MONTH),MONTH) as policy_effective_month
        
        ,sum(written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line) as written_prem_x_ebsl_x_policy_fees
        ,sum(earned_base + earned_total_optionals - earned_optionals_equipment_breakdown - earned_optionals_service_line) as earned_prem_x_ebsl_x_policy_fees
        
        ,sum(earned_base + earned_total_optionals + earned_policy_fee) as earned_prem_inc_ebsl_inc_pol_fees
        ,sum(written_base + written_total_optionals + written_policy_fee) as written_prem_inc_ebsl_inc_pol_fees
        
        ,sum(written_optionals_equipment_breakdown + written_optionals_service_line) as written_EBSL
        ,sum(earned_optionals_equipment_breakdown + earned_optionals_service_line) as earned_EBSL
        
        ,sum(written_exposure) as written_exposure
        ,sum(earned_exposure) as earned_exposure
        
        ,sum(written_policy_fee) as written_policy_fee
        ,sum(earned_policy_fee) as earned_policy_fee
        
        ,sum(expense_load_digital) as written_expense_load
        ,sum(expense_load_digital*earned_exposure) as earned_expense_load
        
        ,sum((written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line - expense_load_digital) * coalesce(on_level_factor,1)) as on_leveled_written_prem_x_ebsl_x_pol_fees_x_exp_load
        ,sum((earned_base + earned_total_optionals - earned_optionals_equipment_breakdown - earned_optionals_service_line - expense_load_digital*earned_exposure) * coalesce(on_level_factor,1)) as on_leveled_earned_prem_x_ebsl_x_pol_fees_x_exp_load
        
from dbt_actuaries.ext_today_knowledge_policy_monthly_premiums_20210331 epud
    left join (select policy_id, on_level_factor, property_data_address_county, property_data_address_zip from dw_staging_extracts.ext_policy_snapshots where date_snapshot = '2021-03-31') seps on seps.policy_id = epud.policy_id
    left join dw_prod.map_expense_loads as exp ON epud.state=exp.state and epud.product=exp.product and epud.carrier = exp.carrier
    left join (select policy_id, date_first_effective, from dw_prod.dim_policies left join dw_prod.dim_policy_groups using (policy_group_id)) dpg on epud.policy_id = dpg.policy_id

        where date_knowledge = '2021-03-31'
        and epud.carrier <> 'canopius'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
, aggregated as (
    select 
        state
        ,carrier
        ,product
        -- ,date_accounting_start as calendar_month_start
        -- ,date_accounting_end as calendar_month_end
        ,last_day(date_trunc(date_accounting_start, QUARTER),QUARTER) as calendar_quarter
        ,calendar_year
        ,reinsurance_treaty_property_accounting
        ,zip
        ,county
        -- ,organization_id
        -- ,channel
        ,tenure
        ,policy_effective_month
        ,CASE
            WHEN product='ho5' then 'spinnaker_non_former_topa'
            WHEN (term_effective >='2020-08-01' and product in ('ho3', 'dp3', 'ho6') and carrier='spinnaker' and state in ('nv', 'wa', 'ut','az','nm','ca','co','or')) 
                then 'spinnaker_former_topa'
            WHEN (term_effective >='2020-08-01' and product in ('ho3', 'dp3', 'ho6') and carrier='spinnaker' and state NOT in ('nv', 'wa', 'ut','az','nm','ca','co','or'))
                then 'spinnaker_non_former_topa'
            WHEN (term_effective <'2020-08-01' and carrier='spinnaker')
                then 'spinnaker_non_former_topa'
            ELSE carrier
        end as carrier_segment
        ,sum(written_prem_x_ebsl_x_policy_fees) as written_prem_x_ebsl_x_policy_fees
        ,sum(earned_prem_x_ebsl_x_policy_fees) as earned_prem_x_ebsl_x_policy_fees
        ,sum(written_prem_inc_ebsl_inc_pol_fees) as written_prem_inc_ebsl_inc_pol_fees
        ,sum(earned_prem_inc_ebsl_inc_pol_fees) as earned_prem_inc_ebsl_inc_pol_fees
        
        ,sum(written_EBSL) as written_EBSL
        ,sum(earned_EBSL) as earned_EBSL

        ,sum(written_exposure) as written_exposure
        ,sum(earned_exposure) as earned_exposure

        ,sum(written_policy_fee) as written_policy_fee
        ,sum(earned_policy_fee) as earned_policy_fee
        
        ,sum(written_expense_load) as written_expense_load
        ,sum(earned_expense_load) as earned_expense_load
        
        ,sum(on_leveled_written_prem_x_ebsl_x_pol_fees_x_exp_load) as on_leveled_written_prem_x_ebsl_x_pol_fees_x_exp_load
        ,sum(on_leveled_earned_prem_x_ebsl_x_pol_fees_x_exp_load) as on_leveled_earned_prem_x_ebsl_x_pol_fees_x_exp_load
        
from premium p
group by 1,2,3,4,5,6,7,8,9,10,11
)
, summary as (
    select 
        reinsurance_treaty_property_accounting,
        sum(written_prem_x_ebsl_x_policy_fees) as written_prem_x_ebsl_x_policy_fees
        ,sum(earned_prem_x_ebsl_x_policy_fees) as earned_prem_x_ebsl_x_policy_fees
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
where 1=1
-- and product <> 'ho5'
and calendar_quarter is not null
-- and calendar_month_start >= '2019-01-01'
and state = 'ca'
-- and reinsurance_treaty_property_accounting = 'spkr21_core'
-- and policy_effective_month is null


