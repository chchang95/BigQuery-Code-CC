with claims_supp as (
select mon.*
, case when cc.cat_ind is true then 'Y'
    when cc.cat_ind is false then 'N'
    when peril = 'wind' or peril = 'hail' then 'Y'
    when cat_code is not null then 'Y'
        else 'N' end as CAT
    , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
    , case when cc.recoded_loss_event is null then 'NA' else cc.recoded_loss_event end as recoded_loss_event
    , case when cc.Updated_Policy_Effective_Date is null then date_effective else cc.Updated_Policy_Effective_Date end as recoded_policy_effective_date
    , case when cc.Updated_Policy_Number is null then claims_policy_number else cc.Updated_Policy_Number end as recoded_policy_number
    , string_field_1 as new_peril_group
    , date_first_effective
    , last_day(date_trunc(date_first_effective, MONTH),MONTH) as policy_effective_month
    , case when cc.updated_treaty is null then reinsurance_treaty else cc.updated_treaty end as recoded_reinsurance_treaty
    , case when tbl_source = 'hippo_claims' then 'Hippo' 
        when tbl_source = 'topa_tpa_claims' then 'TPA'
        when tbl_source = 'spinnaker_tpa_claims' then 'TPA'
        else 'ERROR' end as claims_handler
,coalesce(recoverable_depreciation,0) as total_recoverable_depreciation
,coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) - coalesce(recoveries,0) as loss_incurred_calc
,coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) as expense_incurred_calc
,coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) + coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) - coalesce(recoveries,0) as total_incurred_calc
from dbt_actuaries.ext_all_claims_combined_20210331 mon
left join dbt_actuaries.cat_coding_w_loss_20210331 cc on (case when tbl_source = 'topa_tpa_claims' then ltrim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
left join dbt_actuaries.claims_peril_mappings_202103 map on mon.peril = map.string_field_0
left join (select policy_id, date_first_effective from dw_prod.dim_policies left join dw_prod.dim_policy_groups using (policy_group_id)) using (policy_id)
where mon.date_knowledge = '2021-03-31'
)
,x as (
select 
mon.claim_number,
mon.claims_policy_number,
claims_handler,
last_day(date_trunc(date_knowledge, MONTH),MONTH) as evaluation_date
,last_day(date_trunc(date_knowledge, QUARTER),QUARTER) as calendar_quarter
,date_of_loss
,last_day(date_trunc(date_of_loss, MONTH),MONTH) as accident_month
,last_day(date_trunc(date_of_loss, QUARTER),QUARTER) as accident_quarter
,extract(YEAR from date_of_loss) as accident_year
,recoded_loss_date
,last_day(date_trunc(recoded_loss_date, MONTH),MONTH) as accident_month_recoded
,last_day(date_trunc(recoded_loss_date, QUARTER),QUARTER) as accident_quarter_recoded
,extract(YEAR from recoded_loss_date) as accident_year_recoded
,date_first_notice_of_loss
,last_day(date_trunc(date_first_notice_of_loss,MONTH),MONTH) as report_month
,last_day(date_trunc(date_first_notice_of_loss, QUARTER),QUARTER) as report_quarter
,extract(YEAR from date_first_notice_of_loss) as report_year
,property_data_address_state as state
,property_data_address_zip as zipcode
,carrier
,CASE
    WHEN product='ho5' then 'spinnaker_non_former_topa'
    WHEN (recoded_policy_effective_date >='2020-08-01' and product in ('ho3', 'dp3', 'ho6') and carrier='spinnaker' and property_data_address_state in ('nv', 'wa', 'ut','az','nm','ca','co','or')) 
        then 'spinnaker_former_topa'
    WHEN (recoded_policy_effective_date >='2020-08-01' and product in ('ho3', 'dp3', 'ho6') and carrier='spinnaker' and property_data_address_state NOT in ('nv', 'wa', 'ut','az','nm','ca','co','or'))
        then 'spinnaker_non_former_topa'
    WHEN (recoded_policy_effective_date <'2020-08-01' and carrier='spinnaker')
        then 'spinnaker_non_former_topa'
    ELSE carrier
end as carrier_segment
,product
,is_ebsl
,mon.peril
,new_peril_group
,recoded_loss_event
,case when CAT = 'N' then null
    when (CAT = 'Y' and recoded_loss_event in ('2115_direct', '2116_direct','2117_direct','2115_indirect','2116_indirect','2117_indirect','2115_indeterminate','2117_indeterminate'))
        then '202102_winterstorm'
    when (CAT = 'Y' and recoded_loss_event not in ('2115_direct', '2116_direct','2117_direct','2115_indirect','2116_indirect','2117_indirect','2115_indeterminate','2117_indeterminate'))
        then cm.cat_peril
    end as analysis_peril
,CAT
,reinsurance_treaty
,recoded_reinsurance_treaty
,date_effective as original_date_effective
,recoded_policy_effective_date
,last_day(date_trunc(recoded_policy_effective_date,MONTH),MONTH) as term_policy_effective_month
,date_first_effective
,policy_effective_month
,case when recoded_policy_effective_date = date_first_effective then 'new' else 'renewal' end as tenure
,claim_status
,claim_closed_no_total_payment
,claim_closed_no_loss_payment
,claim_count
,date_closed
, date_diff(mon.date_knowledge,recoded_loss_date,DAY) as loss_dev_age_in_days 
, date_diff(mon.date_knowledge,recoded_loss_date,MONTH) as loss_dev_age_in_mos_by_acc_month_recoded 
, date_diff(date_first_notice_of_loss,recoded_loss_date, DAY) as reporting_lag_level_in_days_recoded
, trunc(date_diff(date_first_notice_of_loss,recoded_loss_date, DAY)/30.5,0) as reporting_within_num_month_lag_level_recoded
, date_diff(last_day(date_trunc(date_knowledge, MONTH),MONTH), date_add(last_day(date_trunc(recoded_loss_date, QUARTER),QUARTER), INTERVAL -3 MONTH), MONTH) as loss_dev_age_in_mos_by_quarter_recoded
, date_diff(last_day(date_trunc(date_knowledge, MONTH),MONTH), date_add(last_day(date_trunc(date_first_notice_of_loss, QUARTER),QUARTER), INTERVAL -3 MONTH), MONTH) as reporting_lag_in_mos_by_quarter

## Total Loss
,sum(loss_paid) as paid_loss_cumulative_prior_to_recoveries_cumulative
,sum(loss_paid-recoveries) as paid_loss_net_recoveries_cumulative
,sum(expense_paid) as paid_expense_cumulative
,sum(total_paid) as paid_total_cumulative
,sum(total_paid-recoveries) as paid_total_net_recoveries_cumulative

,sum(loss_net_reserve) as outstanding_loss_case_reserves
,sum(expense_net_reserve) as outstanding_expense_case_reserves
,sum(total_net_reserve) as outstanding_total_case_reserves

,sum(recoveries) as total_recoveries

,sum(loss_incurred_calc + recoveries) as incurred_loss_prior_to_recoveries_cumulative
,sum(expense_incurred + recoveries) as incurred_expense_prior_to_recoveries_cumulative
,sum(total_incurred + recoveries) as incurred_total_prior_to_recoveries_cumulative
,sum(loss_incurred_calc) as incurred_loss_net_recoveries_cumulative
,sum(expense_incurred_calc) as incurred_expense_net_recoveries_cumulative
,sum(total_incurred_calc) as incurred_total_net_recoveries_cumulative

,sum(total_recoverable_depreciation) as outstanding_recoverable_depreciation

,sum(case when date_closed is null then total_incurred_calc else 0 end) as incurred_total_net_recoveries_cumulative_on_open
,sum(case when claim_closed_no_total_payment is true then 0 else total_incurred_calc end) as incurred_total_net_recoveries_cumulative_on_closed_x_Closed_No_TOTAL_Pay

,sum(claim_count) as reported_claim_count
,sum(case when date_closed is null then claim_count else 0 end) as open_claim_count
,sum(case when date_closed is null then 0 else claim_count end) as closed_claim_count

,sum(case when claim_closed_no_total_payment is true then 0 else claim_count end) as reported_claim_count_x_Closed_No_TOTAL_Pay
,sum(case when claim_closed_no_loss_payment is true then 0 else claim_count end) as reported_claim_count_x_Closed_No_LOSS_Pay

,sum(case when date_closed is null then 0 when claim_closed_no_total_payment is true then 0 else claim_count end) as closed_claim_count_x_Closed_No_TOTAL_Pay
,sum(case when date_closed is null then 0 when claim_closed_no_loss_payment is true then 0 else claim_count end) as closed_claim_count_x_Closed_No_LOSS_Pay
,sum(case when date_closed is null then 0 when claim_closed_no_total_payment is true then claim_count else 0 end) as closed_claim_count_No_TOTAL_Pay
,sum(case when date_closed is null then 0 when claim_closed_no_loss_payment is true then claim_count else 0 end) as closed_claim_count_No_LOSS_Pay

from claims_supp mon
    left join dbt_actuaries.cat_peril_mapping cm on cm.peril=mon.peril
    left join (select policy_number, renewal_number from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2021-03-31') eps on eps.policy_number=mon.recoded_policy_number
where 1=1
and carrier <> 'canopius'
--and is_ebsl is false
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47
),

capped as(
select *
## Capped Loss

,case when date_closed is null then incurred_loss_net_recoveries_cumulative else 0 end as reported_on_open
,case when (date_closed is null or claim_closed_no_total_payment is true) then 0 else incurred_loss_net_recoveries_cumulative end as reported_loss_on_closed_x_Closed_No_TOTAL_Pay
,least(100000, incurred_loss_net_recoveries_cumulative) as capped_incurred_loss_net_recoveries_at_100k
,incurred_loss_net_recoveries_cumulative - least(100000, incurred_loss_net_recoveries_cumulative) as excess_incurred_loss_net_recoveries_at_100k

,case when incurred_loss_net_recoveries_cumulative = 0 then 0 else (paid_loss_net_recoveries_cumulative/ incurred_loss_net_recoveries_cumulative) * least(100000,incurred_loss_net_recoveries_cumulative) end as capped_loss_paid_net_recoveries_at_100k
,case when incurred_loss_net_recoveries_cumulative = 0 then 0 else paid_loss_net_recoveries_cumulative - ((paid_loss_net_recoveries_cumulative / incurred_loss_net_recoveries_cumulative) * least(100000,incurred_loss_net_recoveries_cumulative)) end as excess_loss_paid_net_recoveries_at_100k

,case when incurred_loss_net_recoveries_cumulative = 0 then 0 else (outstanding_loss_case_reserves / incurred_loss_net_recoveries_cumulative) * least(100000,incurred_loss_net_recoveries_cumulative) end as capped_loss_case_reserves_at_100k
,case when incurred_loss_net_recoveries_cumulative = 0 then 0 else outstanding_loss_case_reserves - ((outstanding_loss_case_reserves / incurred_loss_net_recoveries_cumulative) * least(100000,incurred_loss_net_recoveries_cumulative)) end as excess_loss_case_reserves_at_100k

,case when claim_closed_no_total_payment is true then 0 when incurred_total_net_recoveries_cumulative >= 100000 then claim_count else 0 end as reported_claim_count_x_Closed_No_TOTAL_Pay_excess_100k
,case when claim_closed_no_loss_payment is true then 0 when incurred_total_net_recoveries_cumulative >= 100000 then claim_count else 0 end as reported_claim_count_x_Closed_No_LOSS_Pay_excess_100k

from x
),

loss_data as(
select 
CONCAT(state,carrier,product,string(accident_month_recoded),string(accident_quarter_recoded),accident_year_recoded, recoded_reinsurance_treaty,tenure,string(policy_effective_month),carrier_segment) as uniqueKey
,state
,carrier
,product
,accident_month_recoded as calendar_month_end
,accident_quarter_recoded as calendar_quarter
,accident_year_recoded as calendar_year
,recoded_reinsurance_treaty as reinsurance_treaty_property_accounting
,tenure
,policy_effective_month
,carrier_segment
,sum(incurred_total_net_recoveries_cumulative) as incurred_total_net_recoveries
,sum(case when new_peril_group <>"Fire_Smoke" and CAT = "N" and is_ebsl is false then capped_incurred_loss_net_recoveries_at_100k else 0 end) as Reported_Loss_NonCat_xFire
,sum(case when new_peril_group <>"Fire_Smoke" and CAT = "N" and is_ebsl is false then capped_loss_case_reserves_at_100k else 0 end) as Case_Reserves_NonCat_xFire
,sum(case when new_peril_group <>"Fire_Smoke" and CAT = "N" and is_ebsl is false then capped_loss_paid_net_recoveries_at_100k else 0 end) as Paid_Losses_NonCat_xFire
,sum(case when new_peril_group <>"Fire_Smoke" and CAT = "N" and is_ebsl is false then open_claim_count else 0 end) as Open_Counts_NonCat_xFire
,sum(case when new_peril_group <>"Fire_Smoke" and CAT = "N" and is_ebsl is false then reported_claim_count_x_Closed_No_TOTAL_Pay else 0 end) as Reported_Counts_NonCat_xFire
,sum(case when new_peril_group = "Fire_Smoke" and CAT = "N" and is_ebsl is false then capped_incurred_loss_net_recoveries_at_100k else 0 end)as Reported_Loss_NonCat_Fire
,sum(case when new_peril_group = "Fire_Smoke" and CAT = "N" and is_ebsl is false then capped_loss_case_reserves_at_100k else 0 end)as Case_Reserves_NonCat_Fire
,sum(case when new_peril_group = "Fire_Smoke" and CAT = "N" and is_ebsl is false then capped_loss_paid_net_recoveries_at_100k else 0 end)as Paid_Losses_NonCat_Fire
,sum(case when new_peril_group = "Fire_Smoke" and CAT = "N" and is_ebsl is false then open_claim_count else 0 end) as Open_Counts_NonCat_Fire
,sum(case when new_peril_group = "Fire_Smoke" and CAT = "N" and is_ebsl is false then reported_claim_count_x_Closed_No_TOTAL_Pay else 0 end) as Reported_Counts_NonCat_Fire
,sum(case when CAT = "N" and is_ebsl is false then excess_incurred_loss_net_recoveries_at_100k else 0 end) as Reported_Loss_Excess
,sum(case when CAT = "N" and is_ebsl is false then excess_loss_case_reserves_at_100k else 0 end) as Case_Reserves_Excess
,sum(case when CAT = "N" and is_ebsl is false then excess_loss_paid_net_recoveries_at_100k else 0 end) as Paid_Losses_Excess
,sum(case when CAT = "N" and is_ebsl is false then open_claim_count else 0 end) as Open_Counts_Excess
,sum(case when CAT = "N" and is_ebsl is false then reported_claim_count_x_Closed_No_TOTAL_Pay_excess_100k else 0 end) as Reported_Counts_Excess
,sum(case when CAT = "N" and is_ebsl is false then incurred_expense_net_recoveries_cumulative else 0 end) as Reported_Loss_NonCat_ALAE
,sum(case when CAT = "N" and is_ebsl is false then outstanding_expense_case_reserves else 0 end) as Case_Reserves_NonCat_ALAE
,sum(case when CAT = "N" and is_ebsl is false then paid_expense_cumulative else 0 end) as Paid_Losses_NonCat_ALAE
,sum(case when CAT = "N" and is_ebsl is false then open_claim_count else 0 end) as Open_Counts_NonCat_ALAE
,sum(case when CAT = "N" and is_ebsl is false then reported_claim_count else 0 end) as Reported_Counts_NonCat_ALAE
,sum(case when CAT = "Y" and analysis_peril <> "202102_winterstorm" then incurred_expense_net_recoveries_cumulative else 0 end) as Reported_Loss_CATxWS_ALAE
,sum(case when CAT = "Y" and analysis_peril <> "202102_winterstorm" then outstanding_expense_case_reserves else 0 end) as Case_Reserves_CATxWS_ALAE
,sum(case when CAT = "Y" and analysis_peril <> "202102_winterstorm" then paid_expense_cumulative else 0 end) as Paid_Losses_CATxWS_ALAE
,sum(case when CAT = "Y" and analysis_peril <> "202102_winterstorm" then open_claim_count else 0 end) as Open_Counts_CATxWS_ALAE
,sum(case when CAT = "Y" and analysis_peril <> "202102_winterstorm" then reported_claim_count else 0 end) as Reported_Counts_CATxWS_ALAE
,sum(case when CAT = "Y" and analysis_peril <> "202102_winterstorm" then incurred_loss_net_recoveries_cumulative else 0 end) as Reported_Loss_CATxWS
,sum(case when CAT = "Y" and analysis_peril <> "202102_winterstorm" then outstanding_loss_case_reserves else 0 end) as Case_Reserves_CATxWS
,sum(case when CAT = "Y" and analysis_peril <> "202102_winterstorm" then paid_loss_net_recoveries_cumulative else 0 end) as Paid_Losses_CATxWS
,sum(case when CAT = "Y" and analysis_peril <> "202102_winterstorm" then open_claim_count else 0 end) as Open_Counts_CATxWS
,sum(case when CAT = "Y" and analysis_peril <> "202102_winterstorm" then reported_claim_count_x_Closed_No_TOTAL_Pay else 0 end) as Reported_Counts_CATxWS
,sum(case when CAT = "Y" and analysis_peril = "202102_winterstorm" then incurred_expense_net_recoveries_cumulative else 0 end) as Reported_Loss_CAT_WS_ALAE
,sum(case when CAT = "Y" and analysis_peril = "202102_winterstorm" then outstanding_expense_case_reserves else 0 end) as Case_Reserves_CAT_WS_ALAE
,sum(case when CAT = "Y" and analysis_peril = "202102_winterstorm" then paid_expense_cumulative else 0 end) as Paid_Losses_CAT_WS_ALAE
,sum(case when CAT = "Y" and analysis_peril = "202102_winterstorm" then open_claim_count else 0 end) as Open_Counts_CAT_WS_ALAE
,sum(case when CAT = "Y" and analysis_peril = "202102_winterstorm" then reported_claim_count else 0 end) as Reported_Counts_CAT_WS_ALAE
,sum(case when CAT = "Y" and analysis_peril = "202102_winterstorm" then incurred_loss_net_recoveries_cumulative else 0 end) as Reported_Loss_CAT_WS
,sum(case when CAT = "Y" and analysis_peril = "202102_winterstorm" then outstanding_loss_case_reserves else 0 end) as Case_Reserves_CAT_WS
,sum(case when CAT = "Y" and analysis_peril = "202102_winterstorm" then paid_loss_net_recoveries_cumulative else 0 end) as Paid_Losses_CAT_WS
,sum(case when CAT = "Y" and analysis_peril = "202102_winterstorm" then open_claim_count else 0 end) as Open_Counts_CAT_WS
,sum(case when CAT = "Y" and analysis_peril = "202102_winterstorm" then reported_claim_count_x_Closed_No_TOTAL_Pay else 0 end) as Reported_Counts_CAT_WS
,sum(case when CAT = "N" and is_ebsl is true then incurred_expense_net_recoveries_cumulative else 0 end) as Reported_Loss_EBSL_ALAE
,sum(case when CAT = "N" and is_ebsl is true then outstanding_expense_case_reserves else 0 end) as Case_Reserves_EBSL_ALAE
,sum(case when CAT = "N" and is_ebsl is true then paid_expense_cumulative else 0 end) as Paid_Losses_EBSL_ALAE
,sum(case when CAT = "N" and is_ebsl is true then open_claim_count else 0 end) as Open_Counts_EBSL_ALAE
,sum(case when CAT = "N" and is_ebsl is true then reported_claim_count else 0 end) as Reported_Counts_EBSL_ALAE
,sum(case when CAT = "N" and is_ebsl is true then incurred_loss_net_recoveries_cumulative else 0 end) as Reported_Loss_EBSL
,sum(case when CAT = "N" and is_ebsl is true then outstanding_loss_case_reserves else 0 end) as Case_Reserves_EBSL
,sum(case when CAT = "N" and is_ebsl is true then paid_loss_net_recoveries_cumulative else 0 end) as Paid_Losses_EBSL
,sum(case when CAT = "N" and is_ebsl is true then open_claim_count else 0 end) as Open_Counts_EBSL
,sum(case when CAT = "N" and is_ebsl is true then reported_claim_count_x_Closed_No_TOTAL_Pay else 0 end) as Reported_Counts_EBSL
from capped
group by 1,2,3,4,5,6,7,8,9,10,11
),

premium as (
    select 
        epud.policy_id
        ,lower(epud.state) as state
        ,lower(epud.carrier) as carrier
        ,lower(epud.product) as product
        ,extract(year from date_calendar_month_accounting_basis) as calendar_year
        ,date_calendar_month_accounting_basis as date_accounting_start
        ,date_sub(date_add(date_calendar_month_accounting_basis, INTERVAL 1 MONTH), INTERVAL 1 DAY) as date_accounting_end
        ,reinsurance_treaty_property_accounting
        ,case when renewal_number = 0 then "new" else "renewal" end as tenure
        ,date_effective as term_effective
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
        
from dw_prod_extracts.ext_policy_monthly_premiums epud
    left join (select policy_id, on_level_factor from dw_staging_extracts.ext_policy_snapshots where date_snapshot = '2021-03-31') seps on seps.policy_id = epud.policy_id
    left join dw_prod.map_expense_loads as exp ON epud.state=exp.state and epud.product=exp.product and epud.carrier = exp.carrier
    left join (select policy_id, date_first_effective from dw_prod.dim_policies left join dw_prod.dim_policy_groups using (policy_group_id)) dpg on epud.policy_id = dpg.policy_id

        where date_knowledge = '2021-03-31'
        and epud.carrier <> 'canopius'
group by 1,2,3,4,5,6,7,8,9,10,11
)
, aggregated as (
    select 
        state
        ,carrier
        ,product
        ,date_accounting_start as calendar_month_start
        ,date_accounting_end as calendar_month_end
        ,last_day(date_trunc(date_accounting_start, QUARTER),QUARTER) as calendar_quarter
        ,calendar_year
        ,reinsurance_treaty_property_accounting
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
),

premium_data as (
select
CONCAT(state,carrier,product,string(calendar_month_end),string(calendar_quarter), calendar_year, reinsurance_treaty_property_accounting,tenure,string(policy_effective_month),carrier_segment) as uniqueKey
,state
,carrier
,product
,calendar_month_end
,calendar_quarter
,calendar_year
,reinsurance_treaty_property_accounting
,tenure
,policy_effective_month
,carrier_segment
,sum(written_prem_x_ebsl_x_policy_fees) as written_prem_x_ebsl_x_policy_fees
,sum(earned_prem_x_ebsl_x_policy_fees) as earned_prem_x_ebsl_x_policy_fees
,sum(written_exposure) as written_exposure
,sum(earned_exposure) as earned_exposure
from aggregated
group by 1,2,3,4,5,6,7,8,9,10,11),

full_segment as(
select distinct
uniqueKey
,state
,carrier
,product
,calendar_month_end
,calendar_quarter
,calendar_year
,reinsurance_treaty_property_accounting
,tenure
,policy_effective_month
,carrier_segment
from premium_data

UNION ALL

select distinct
uniqueKey
,state
,carrier
,product
,calendar_month_end
,calendar_quarter
,calendar_year
,reinsurance_treaty_property_accounting
,tenure
,policy_effective_month
,carrier_segment
from loss_data
),

dist_full_segment as(
select distinct * from full_segment)
, full_data as(
select
coalesce(l.uniqueKey, s.uniqueKey) as uniqueKey
,coalesce(l.state, s.state) as state
,coalesce(l.carrier, s.carrier) as carrier
,coalesce(l.product, s.product) as product
,coalesce(l.calendar_month_end, s.calendar_month_end) as calendar_month_end
,coalesce(l.calendar_quarter, s.calendar_quarter) as calendar_quarter
,coalesce(l.calendar_year, s.calendar_year) as calendar_year
,coalesce(l.reinsurance_treaty_property_accounting, s.reinsurance_treaty_property_accounting) as reinsurance_treaty_property_accounting
,coalesce(l.tenure, s.tenure) as tenure
,coalesce(l.policy_effective_month, s.policy_effective_month) as policy_effective_month
,coalesce(l.carrier_segment, s.carrier_segment) as carrier_segment
,written_prem_x_ebsl_x_policy_fees
,written_exposure
,earned_prem_x_ebsl_x_policy_fees
,earned_exposure
,incurred_total_net_recoveries
,Reported_Loss_NonCat_xFire
,Case_Reserves_NonCat_xFire
,Paid_Losses_NonCat_xFire
,Open_Counts_NonCat_xFire
,Reported_Counts_NonCat_xFire
,Reported_Loss_NonCat_Fire
,Case_Reserves_NonCat_Fire
,Paid_Losses_NonCat_Fire
,Open_Counts_NonCat_Fire
,Reported_Counts_NonCat_Fire
,Reported_Loss_Excess
,Case_Reserves_Excess
,Paid_Losses_Excess
,Open_Counts_Excess
,Reported_Counts_Excess
,Reported_Loss_NonCat_ALAE
,Case_Reserves_NonCat_ALAE
,Paid_Losses_NonCat_ALAE
,Open_Counts_NonCat_ALAE
,Reported_Counts_NonCat_ALAE
,Reported_Loss_CATxWS_ALAE
,Case_Reserves_CATxWS_ALAE
,Paid_Losses_CATxWS_ALAE
,Open_Counts_CATxWS_ALAE
,Reported_Counts_CATxWS_ALAE
,Reported_Loss_CATxWS
,Case_Reserves_CATxWS
,Paid_Losses_CATxWS
,Open_Counts_CATxWS
,Reported_Counts_CATxWS
,Reported_Loss_CAT_WS_ALAE
,Case_Reserves_CAT_WS_ALAE
,Paid_Losses_CAT_WS_ALAE
,Open_Counts_CAT_WS_ALAE
,Reported_Counts_CAT_WS_ALAE
,Reported_Loss_CAT_WS
,Case_Reserves_CAT_WS
,Paid_Losses_CAT_WS
,Open_Counts_CAT_WS
,Reported_Counts_CAT_WS
,Reported_Loss_EBSL_ALAE
,Case_Reserves_EBSL_ALAE
,Paid_Losses_EBSL_ALAE
,Open_Counts_EBSL_ALAE
,Reported_Counts_EBSL_ALAE
,Reported_Loss_EBSL
,Case_Reserves_EBSL
,Paid_Losses_EBSL
,Open_Counts_EBSL
,Reported_Counts_EBSL
from dist_full_segment s
left join (select uniqueKey, written_prem_x_ebsl_x_policy_fees, written_exposure, earned_prem_x_ebsl_x_policy_fees, earned_exposure from premium_data) p ON s.uniqueKey=p.uniqueKey
left join loss_data l on s.uniqueKey = l.uniqueKey)
, with_window_totals as (
select 
state
,carrier
,product
,calendar_month_end
,calendar_quarter
,calendar_year
,reinsurance_treaty_property_accounting
,tenure
,policy_effective_month
,carrier_segment

# Incrementals
,written_prem_x_ebsl_x_policy_fees
,written_exposure
,earned_prem_x_ebsl_x_policy_fees
,earned_exposure
,incurred_total_net_recoveries
,Reported_Loss_NonCat_xFire
,Case_Reserves_NonCat_xFire
,Paid_Losses_NonCat_xFire
,Open_Counts_NonCat_xFire
,Reported_Counts_NonCat_xFire
,Reported_Loss_NonCat_Fire
,Case_Reserves_NonCat_Fire
,Paid_Losses_NonCat_Fire
,Open_Counts_NonCat_Fire
,Reported_Counts_NonCat_Fire
,Reported_Loss_Excess
,Case_Reserves_Excess
,Paid_Losses_Excess
,Open_Counts_Excess
,Reported_Counts_Excess
,Reported_Loss_NonCat_ALAE
,Case_Reserves_NonCat_ALAE
,Paid_Losses_NonCat_ALAE
,Open_Counts_NonCat_ALAE
,Reported_Counts_NonCat_ALAE
,Reported_Loss_CATxWS_ALAE
,Case_Reserves_CATxWS_ALAE
,Paid_Losses_CATxWS_ALAE
,Open_Counts_CATxWS_ALAE
,Reported_Counts_CATxWS_ALAE
,Reported_Loss_CATxWS
,Case_Reserves_CATxWS
,Paid_Losses_CATxWS
,Open_Counts_CATxWS
,Reported_Counts_CATxWS
,Reported_Loss_CAT_WS_ALAE
,Case_Reserves_CAT_WS_ALAE
,Paid_Losses_CAT_WS_ALAE
,Open_Counts_CAT_WS_ALAE
,Reported_Counts_CAT_WS_ALAE
,Reported_Loss_CAT_WS
,Case_Reserves_CAT_WS
,Paid_Losses_CAT_WS
,Open_Counts_CAT_WS
,Reported_Counts_CAT_WS
,Reported_Loss_EBSL_ALAE
,Case_Reserves_EBSL_ALAE
,Paid_Losses_EBSL_ALAE
,Open_Counts_EBSL_ALAE
,Reported_Counts_EBSL_ALAE
,Reported_Loss_EBSL
,Case_Reserves_EBSL
,Paid_Losses_EBSL
,Open_Counts_EBSL
,Reported_Counts_EBSL

# Window Totals
,sum(written_prem_x_ebsl_x_policy_fees) over (partition by carrier, calendar_quarter, carrier_segment) as written_prem_x_ebsl_x_policy_fees_sum
,sum(written_exposure) over (partition by carrier, calendar_quarter, carrier_segment) as written_exposure_sum
,sum(earned_prem_x_ebsl_x_policy_fees) over (partition by carrier, calendar_quarter, carrier_segment) as earned_prem_x_ebsl_x_policy_fees_sum
,sum(earned_exposure) over (partition by carrier, calendar_quarter, carrier_segment) as earned_exposure_sum
,sum(Reported_Loss_NonCat_xFire) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Loss_NonCat_xFire_sum
,sum(Case_Reserves_NonCat_xFire) over (partition by carrier, calendar_quarter, carrier_segment) as Case_Reserves_NonCat_xFire_sum
,sum(Paid_Losses_NonCat_xFire) over (partition by carrier, calendar_quarter, carrier_segment) as Paid_Losses_NonCat_xFire_sum
,sum(Open_Counts_NonCat_xFire) over (partition by carrier, calendar_quarter, carrier_segment) as Open_Counts_NonCat_xFire_sum
,sum(Reported_Counts_NonCat_xFire) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Counts_NonCat_xFire_sum
,sum(Reported_Loss_NonCat_Fire) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Loss_NonCat_Fire_sum
,sum(Case_Reserves_NonCat_Fire) over (partition by carrier, calendar_quarter, carrier_segment) as Case_Reserves_NonCat_Fire_sum
,sum(Paid_Losses_NonCat_Fire) over (partition by carrier, calendar_quarter, carrier_segment) as Paid_Losses_NonCat_Fire_sum
,sum(Open_Counts_NonCat_Fire) over (partition by carrier, calendar_quarter, carrier_segment) as Open_Counts_NonCat_Fire_sum
,sum(Reported_Counts_NonCat_Fire) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Counts_NonCat_Fire_sum
,sum(Reported_Loss_Excess) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Loss_Excess_sum
,sum(Case_Reserves_Excess) over (partition by carrier, calendar_quarter, carrier_segment) as Case_Reserves_Excess_sum
,sum(Paid_Losses_Excess) over (partition by carrier, calendar_quarter, carrier_segment) as Paid_Losses_Excess_sum
,sum(Open_Counts_Excess) over (partition by carrier, calendar_quarter, carrier_segment) as Open_Counts_Excess_sum
,sum(Reported_Counts_Excess) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Counts_Excess_sum
,sum(Reported_Loss_NonCat_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Loss_NonCat_ALAE_sum
,sum(Case_Reserves_NonCat_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Case_Reserves_NonCat_ALAE_sum
,sum(Paid_Losses_NonCat_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Paid_Losses_NonCat_ALAE_sum
,sum(Open_Counts_NonCat_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Open_Counts_NonCat_ALAE_sum
,sum(Reported_Counts_NonCat_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Counts_NonCat_ALAE_sum
,sum(Reported_Loss_CATxWS_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Loss_CATxWS_ALAE_sum
,sum(Case_Reserves_CATxWS_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Case_Reserves_CATxWS_ALAE_sum
,sum(Paid_Losses_CATxWS_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Paid_Losses_CATxWS_ALAE_sum
,sum(Open_Counts_CATxWS_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Open_Counts_CATxWS_ALAE_sum
,sum(Reported_Counts_CATxWS_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Counts_CATxWS_ALAE_sum
,sum(Reported_Loss_CATxWS) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Loss_CATxWS_sum
,sum(Case_Reserves_CATxWS) over (partition by carrier, calendar_quarter, carrier_segment) as Case_Reserves_CATxWS_sum
,sum(Paid_Losses_CATxWS) over (partition by carrier, calendar_quarter, carrier_segment) as Paid_Losses_CATxWS_sum
,sum(Open_Counts_CATxWS) over (partition by carrier, calendar_quarter, carrier_segment) as Open_Counts_CATxWS_sum
,sum(Reported_Counts_CATxWS) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Counts_CATxWS_sum
,sum(Reported_Loss_CAT_WS_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Loss_CAT_WS_ALAE_sum
,sum(Case_Reserves_CAT_WS_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Case_Reserves_CAT_WS_ALAE_sum
,sum(Paid_Losses_CAT_WS_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Paid_Losses_CAT_WS_ALAE_sum
,sum(Open_Counts_CAT_WS_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Open_Counts_CAT_WS_ALAE_sum
,sum(Reported_Counts_CAT_WS_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Counts_CAT_WS_ALAE_sum
,sum(Reported_Loss_CAT_WS) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Loss_CAT_WS_sum
,sum(Case_Reserves_CAT_WS) over (partition by carrier, calendar_quarter, carrier_segment) as Case_Reserves_CAT_WS_sum
,sum(Paid_Losses_CAT_WS) over (partition by carrier, calendar_quarter, carrier_segment) as Paid_Losses_CAT_WS_sum
,sum(Open_Counts_CAT_WS) over (partition by carrier, calendar_quarter, carrier_segment) as Open_Counts_CAT_WS_sum
,sum(Reported_Counts_CAT_WS) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Counts_CAT_WS_sum
,sum(Reported_Loss_EBSL_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Loss_EBSL_ALAE_sum
,sum(Case_Reserves_EBSL_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Case_Reserves_EBSL_ALAE_sum
,sum(Paid_Losses_EBSL_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Paid_Losses_EBSL_ALAE_sum
,sum(Open_Counts_EBSL_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Open_Counts_EBSL_ALAE_sum
,sum(Reported_Counts_EBSL_ALAE) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Counts_EBSL_ALAE_sum
,sum(Reported_Loss_EBSL) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Loss_EBSL_sum
,sum(Case_Reserves_EBSL) over (partition by carrier, calendar_quarter, carrier_segment) as Case_Reserves_EBSL_sum
,sum(Paid_Losses_EBSL) over (partition by carrier, calendar_quarter, carrier_segment) as Paid_Losses_EBSL_sum
,sum(Open_Counts_EBSL) over (partition by carrier, calendar_quarter, carrier_segment) as Open_Counts_EBSL_sum
,sum(Reported_Counts_EBSL) over (partition by carrier, calendar_quarter, carrier_segment) as Reported_Counts_EBSL_sum
from full_data
where incurred_total_net_recoveries is not null or written_prem_x_ebsl_x_policy_fees + earned_prem_x_ebsl_x_policy_fees + earned_exposure is not null
order by state, product desc
)
select 
state
,carrier
,product
,calendar_month_end
,calendar_quarter
,calendar_year
,reinsurance_treaty_property_accounting
,tenure
,policy_effective_month
,carrier_segment

# Incrementals
,written_prem_x_ebsl_x_policy_fees
,written_exposure
,earned_prem_x_ebsl_x_policy_fees
,earned_exposure
,incurred_total_net_recoveries
,Reported_Loss_NonCat_xFire
,Case_Reserves_NonCat_xFire
,Paid_Losses_NonCat_xFire
,Open_Counts_NonCat_xFire
,Reported_Counts_NonCat_xFire
,Reported_Loss_NonCat_Fire
,Case_Reserves_NonCat_Fire
,Paid_Losses_NonCat_Fire
,Open_Counts_NonCat_Fire
,Reported_Counts_NonCat_Fire
,Reported_Loss_Excess
,Case_Reserves_Excess
,Paid_Losses_Excess
,Open_Counts_Excess
,Reported_Counts_Excess
,Reported_Loss_NonCat_ALAE
,Case_Reserves_NonCat_ALAE
,Paid_Losses_NonCat_ALAE
,Open_Counts_NonCat_ALAE
,Reported_Counts_NonCat_ALAE
,Reported_Loss_CATxWS_ALAE
,Case_Reserves_CATxWS_ALAE
,Paid_Losses_CATxWS_ALAE
,Open_Counts_CATxWS_ALAE
,Reported_Counts_CATxWS_ALAE
,Reported_Loss_CATxWS
,Case_Reserves_CATxWS
,Paid_Losses_CATxWS
,Open_Counts_CATxWS
,Reported_Counts_CATxWS
,Reported_Loss_CAT_WS_ALAE
,Case_Reserves_CAT_WS_ALAE
,Paid_Losses_CAT_WS_ALAE
,Open_Counts_CAT_WS_ALAE
,Reported_Counts_CAT_WS_ALAE
,Reported_Loss_CAT_WS
,Case_Reserves_CAT_WS
,Paid_Losses_CAT_WS
,Open_Counts_CAT_WS
,Reported_Counts_CAT_WS
,Reported_Loss_EBSL_ALAE
,Case_Reserves_EBSL_ALAE
,Paid_Losses_EBSL_ALAE
,Open_Counts_EBSL_ALAE
,Reported_Counts_EBSL_ALAE
,Reported_Loss_EBSL
,Case_Reserves_EBSL
,Paid_Losses_EBSL
,Open_Counts_EBSL
,Reported_Counts_EBSL

# Percents
, case when written_prem_x_ebsl_x_policy_fees_sum = 0 then 0 else written_prem_x_ebsl_x_policy_fees / written_prem_x_ebsl_x_policy_fees_sum end as written_prem_x_ebsl_x_policy_fees_percent
, case when written_exposure_sum = 0 then 0 else written_exposure / written_exposure_sum end as written_exposure_percent
, case when earned_prem_x_ebsl_x_policy_fees_sum = 0 then 0 else earned_prem_x_ebsl_x_policy_fees / earned_prem_x_ebsl_x_policy_fees_sum end as earned_prem_x_ebsl_x_policy_fees_percent
, case when earned_exposure_sum = 0 then 0 else earned_exposure / earned_exposure_sum end as earned_exposure_percent
, case when Reported_Loss_NonCat_xFire_sum = 0 then 0 else Reported_Loss_NonCat_xFire / Reported_Loss_NonCat_xFire_sum end as Reported_Loss_NonCat_xFire_percent
, case when Case_Reserves_NonCat_xFire_sum = 0 then 0 else Case_Reserves_NonCat_xFire / Case_Reserves_NonCat_xFire_sum end as Case_Reserves_NonCat_xFire_percent
, case when Paid_Losses_NonCat_xFire_sum = 0 then 0 else Paid_Losses_NonCat_xFire / Paid_Losses_NonCat_xFire_sum end as Paid_Losses_NonCat_xFire_percent
, case when Open_Counts_NonCat_xFire_sum = 0 then 0 else Open_Counts_NonCat_xFire / Open_Counts_NonCat_xFire_sum end as Open_Counts_NonCat_xFire_percent
, case when Reported_Counts_NonCat_xFire_sum = 0 then 0 else Reported_Counts_NonCat_xFire / Reported_Counts_NonCat_xFire_sum end as Reported_Counts_NonCat_xFire_percent
, case when Reported_Loss_NonCat_Fire_sum = 0 then 0 else Reported_Loss_NonCat_Fire / Reported_Loss_NonCat_Fire_sum end as Reported_Loss_NonCat_Fire_percent
, case when Case_Reserves_NonCat_Fire_sum = 0 then 0 else Case_Reserves_NonCat_Fire / Case_Reserves_NonCat_Fire_sum end as Case_Reserves_NonCat_Fire_percent
, case when Paid_Losses_NonCat_Fire_sum = 0 then 0 else Paid_Losses_NonCat_Fire / Paid_Losses_NonCat_Fire_sum end as Paid_Losses_NonCat_Fire_percent
, case when Open_Counts_NonCat_Fire_sum = 0 then 0 else Open_Counts_NonCat_Fire / Open_Counts_NonCat_Fire_sum end as Open_Counts_NonCat_Fire_percent
, case when Reported_Counts_NonCat_Fire_sum = 0 then 0 else Reported_Counts_NonCat_Fire / Reported_Counts_NonCat_Fire_sum end as Reported_Counts_NonCat_Fire_percent
, case when Reported_Loss_Excess_sum = 0 then 0 else Reported_Loss_Excess / Reported_Loss_Excess_sum end as Reported_Loss_Excess_percent
, case when Case_Reserves_Excess_sum = 0 then 0 else Case_Reserves_Excess / Case_Reserves_Excess_sum end as Case_Reserves_Excess_percent
, case when Paid_Losses_Excess_sum = 0 then 0 else Paid_Losses_Excess / Paid_Losses_Excess_sum end as Paid_Losses_Excess_percent
, case when Open_Counts_Excess_sum = 0 then 0 else Open_Counts_Excess / Open_Counts_Excess_sum end as Open_Counts_Excess_percent
, case when Reported_Counts_Excess_sum = 0 then 0 else Reported_Counts_Excess / Reported_Counts_Excess_sum end as Reported_Counts_Excess_percent
, case when Reported_Loss_NonCat_ALAE_sum = 0 then 0 else Reported_Loss_NonCat_ALAE / Reported_Loss_NonCat_ALAE_sum end as Reported_Loss_NonCat_ALAE_percent
, case when Case_Reserves_NonCat_ALAE_sum = 0 then 0 else Case_Reserves_NonCat_ALAE / Case_Reserves_NonCat_ALAE_sum end as Case_Reserves_NonCat_ALAE_percent
, case when Paid_Losses_NonCat_ALAE_sum = 0 then 0 else Paid_Losses_NonCat_ALAE / Paid_Losses_NonCat_ALAE_sum end as Paid_Losses_NonCat_ALAE_percent
, case when Open_Counts_NonCat_ALAE_sum = 0 then 0 else Open_Counts_NonCat_ALAE / Open_Counts_NonCat_ALAE_sum end as Open_Counts_NonCat_ALAE_percent
, case when Reported_Counts_NonCat_ALAE_sum = 0 then 0 else Reported_Counts_NonCat_ALAE / Reported_Counts_NonCat_ALAE_sum end as Reported_Counts_NonCat_ALAE_percent
, case when Reported_Loss_CATxWS_ALAE_sum = 0 then 0 else Reported_Loss_CATxWS_ALAE / Reported_Loss_CATxWS_ALAE_sum end as Reported_Loss_CATxWS_ALAE_percent
, case when Case_Reserves_CATxWS_ALAE_sum = 0 then 0 else Case_Reserves_CATxWS_ALAE / Case_Reserves_CATxWS_ALAE_sum end as Case_Reserves_CATxWS_ALAE_percent
, case when Paid_Losses_CATxWS_ALAE_sum = 0 then 0 else Paid_Losses_CATxWS_ALAE / Paid_Losses_CATxWS_ALAE_sum end as Paid_Losses_CATxWS_ALAE_percent
, case when Open_Counts_CATxWS_ALAE_sum = 0 then 0 else Open_Counts_CATxWS_ALAE / Open_Counts_CATxWS_ALAE_sum end as Open_Counts_CATxWS_ALAE_percent
, case when Reported_Counts_CATxWS_ALAE_sum = 0 then 0 else Reported_Counts_CATxWS_ALAE / Reported_Counts_CATxWS_ALAE_sum end as Reported_Counts_CATxWS_ALAE_percent
, case when Reported_Loss_CATxWS_sum = 0 then 0 else Reported_Loss_CATxWS / Reported_Loss_CATxWS_sum end as Reported_Loss_CATxWS_percent
, case when Case_Reserves_CATxWS_sum = 0 then 0 else Case_Reserves_CATxWS / Case_Reserves_CATxWS_sum end as Case_Reserves_CATxWS_percent
, case when Paid_Losses_CATxWS_sum = 0 then 0 else Paid_Losses_CATxWS / Paid_Losses_CATxWS_sum end as Paid_Losses_CATxWS_percent
, case when Open_Counts_CATxWS_sum = 0 then 0 else Open_Counts_CATxWS / Open_Counts_CATxWS_sum end as Open_Counts_CATxWS_percent
, case when Reported_Counts_CATxWS_sum = 0 then 0 else Reported_Counts_CATxWS / Reported_Counts_CATxWS_sum end as Reported_Counts_CATxWS_percent
, case when Reported_Loss_CAT_WS_ALAE_sum = 0 then 0 else Reported_Loss_CAT_WS_ALAE / Reported_Loss_CAT_WS_ALAE_sum end as Reported_Loss_CAT_WS_ALAE_percent
, case when Case_Reserves_CAT_WS_ALAE_sum = 0 then 0 else Case_Reserves_CAT_WS_ALAE / Case_Reserves_CAT_WS_ALAE_sum end as Case_Reserves_CAT_WS_ALAE_percent
, case when Paid_Losses_CAT_WS_ALAE_sum = 0 then 0 else Paid_Losses_CAT_WS_ALAE / Paid_Losses_CAT_WS_ALAE_sum end as Paid_Losses_CAT_WS_ALAE_percent
, case when Open_Counts_CAT_WS_ALAE_sum = 0 then 0 else Open_Counts_CAT_WS_ALAE / Open_Counts_CAT_WS_ALAE_sum end as Open_Counts_CAT_WS_ALAE_percent
, case when Reported_Counts_CAT_WS_ALAE_sum = 0 then 0 else Reported_Counts_CAT_WS_ALAE / Reported_Counts_CAT_WS_ALAE_sum end as Reported_Counts_CAT_WS_ALAE_percent
, case when Reported_Loss_CAT_WS_sum = 0 then 0 else Reported_Loss_CAT_WS / Reported_Loss_CAT_WS_sum end as Reported_Loss_CAT_WS_percent
, case when Case_Reserves_CAT_WS_sum = 0 then 0 else Case_Reserves_CAT_WS / Case_Reserves_CAT_WS_sum end as Case_Reserves_CAT_WS_percent
, case when Paid_Losses_CAT_WS_sum = 0 then 0 else Paid_Losses_CAT_WS / Paid_Losses_CAT_WS_sum end as Paid_Losses_CAT_WS_percent
, case when Open_Counts_CAT_WS_sum = 0 then 0 else Open_Counts_CAT_WS / Open_Counts_CAT_WS_sum end as Open_Counts_CAT_WS_percent
, case when Reported_Counts_CAT_WS_sum = 0 then 0 else Reported_Counts_CAT_WS / Reported_Counts_CAT_WS_sum end as Reported_Counts_CAT_WS_percent
, case when Reported_Loss_EBSL_ALAE_sum = 0 then 0 else Reported_Loss_EBSL_ALAE / Reported_Loss_EBSL_ALAE_sum end as Reported_Loss_EBSL_ALAE_percent
, case when Case_Reserves_EBSL_ALAE_sum = 0 then 0 else Case_Reserves_EBSL_ALAE / Case_Reserves_EBSL_ALAE_sum end as Case_Reserves_EBSL_ALAE_percent
, case when Paid_Losses_EBSL_ALAE_sum = 0 then 0 else Paid_Losses_EBSL_ALAE / Paid_Losses_EBSL_ALAE_sum end as Paid_Losses_EBSL_ALAE_percent
, case when Open_Counts_EBSL_ALAE_sum = 0 then 0 else Open_Counts_EBSL_ALAE / Open_Counts_EBSL_ALAE_sum end as Open_Counts_EBSL_ALAE_percent
, case when Reported_Counts_EBSL_ALAE_sum = 0 then 0 else Reported_Counts_EBSL_ALAE / Reported_Counts_EBSL_ALAE_sum end as Reported_Counts_EBSL_ALAE_percent
, case when Reported_Loss_EBSL_sum = 0 then 0 else Reported_Loss_EBSL / Reported_Loss_EBSL_sum end as Reported_Loss_EBSL_percent
, case when Case_Reserves_EBSL_sum = 0 then 0 else Case_Reserves_EBSL / Case_Reserves_EBSL_sum end as Case_Reserves_EBSL_percent
, case when Paid_Losses_EBSL_sum = 0 then 0 else Paid_Losses_EBSL / Paid_Losses_EBSL_sum end as Paid_Losses_EBSL_percent
, case when Open_Counts_EBSL_sum = 0 then 0 else Open_Counts_EBSL / Open_Counts_EBSL_sum end as Open_Counts_EBSL_percent
, case when Reported_Counts_EBSL_sum = 0 then 0 else Reported_Counts_EBSL / Reported_Counts_EBSL_sum end as Reported_Counts_EBSL_percent

from with_window_totals


