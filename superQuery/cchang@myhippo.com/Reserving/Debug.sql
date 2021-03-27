with claims_supp as (
select mon.*
, case when cc.cat_ind is true then 'Y'
    when cc.cat_ind is false then 'N'
    when peril = 'wind' or peril = 'hail' then 'Y'
    when cat_code is not null then 'Y'
        else 'N' end as CAT
    , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
    , case when cc.recoded_loss_event is null then 'NA' else cc.recoded_loss_event end as recoded_loss_event
    , string_field_1 as new_peril_group
    , last_day(date_trunc(date_effective,MONTH),MONTH) as term_policy_effective_month
    , last_day(date_trunc(date_first_effective, MONTH),MONTH) as policy_effective_month
    , case when renewal_number > 0 then 'renewal' else 'new' end as tenure
    , case when cc.updated_treaty is null then reinsurance_treaty else cc.updated_treaty end as recoded_reinsurance_treaty
,coalesce(recoverable_depreciation,0) as total_recoverable_depreciation
,coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) - coalesce(recoveries,0) as loss_incurred_calc
,coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) as expense_incurred_calc
,coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) + coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) - coalesce(recoveries,0) as total_incurred_calc
from dw_prod_extracts.ext_all_claims_combined mon
left join dbt_actuaries.cat_coding_w_loss_20210322_treaty cc on (case when tbl_source = 'topa_tpa_claims' then ltrim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
left join (select policy_id, renewal_number from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2021-03-26') using(policy_id)
left join dbt_actuaries.claims_peril_mappings_202103 map on mon.peril = map.string_field_0
left join (select policy_id, date_first_effective from dw_prod.dim_policies left join dw_prod.dim_policy_groups using (policy_group_id)) using (policy_id)
where (mon.date_knowledge = last_day(date_trunc(mon.date_knowledge, QUARTER),QUARTER) or mon.date_knowledge in ('2021-03-26'))
and mon.claim_number<>'009077-000128-GD-01'
--and is_ebsl=true
)
,x as (
select 
mon.claim_number,
mon.claims_policy_number,
last_day(date_trunc(date_knowledge, MONTH),MONTH) as evaluation_date
,last_day(date_trunc(date_knowledge, QUARTER),QUARTER) as calendar_quarter
-- ,extract(YEAR from date_knowledge) as calendar_year
,last_day(date_trunc(date_of_loss, MONTH),MONTH) as accident_month
,last_day(date_trunc(date_of_loss, QUARTER),QUARTER) as accident_quarter
,extract(YEAR from date_of_loss) as accident_year
,last_day(date_trunc(recoded_loss_date, MONTH),MONTH) as accident_month_recoded
,last_day(date_trunc(recoded_loss_date, QUARTER),QUARTER) as accident_quarter_recoded
,extract(YEAR from recoded_loss_date) as accident_year_recoded
,last_day(date_trunc(date_first_notice_of_loss,MONTH),MONTH) as report_month
,last_day(date_trunc(date_first_notice_of_loss, QUARTER),QUARTER) as report_quarter
,extract(YEAR from date_first_notice_of_loss) as report_year
,property_data_address_state as state
,carrier
,CASE
    WHEN product='ho5' then 'spinnaker_non_former_topa'
    WHEN (policy_effective_month >='2020-08-01' and product in ('ho3', 'dp3', 'ho6') and carrier='spinnaker' and property_data_address_state in ('nv', 'wa', 'ut','az','nm','ca','co','or')) 
        then 'spinnaker_former_topa'
    WHEN (policy_effective_month >='2020-08-01' and product in ('ho3', 'dp3', 'ho6') and carrier='spinnaker' and property_data_address_state NOT in ('nv', 'wa', 'ut','az','nm','ca','co','or'))
        then 'spinnaker_non_former_topa'
    WHEN (policy_effective_month <'2020-08-01' and carrier='spinnaker')
        then 'spinnaker_non_former_topa'
    ELSE carrier
end as carrier_segment
,product
,is_ebsl
,peril
,new_peril_group
,case when recoded_loss_event in ('2115_direct', '2116_direct','2117_direct','2115_indirect','2116_indirect','2117_indirect','2115_indeterminate','2117_indeterminate')
    then '202102_winterstorm'
    when new_peril_group in ('Wind', 'Hurricane') then 'Wind'
    when new_peril_group = 'Hail' then 'Hail'
    else 'All_other'
    end as analysis_peril
,CAT
,reinsurance_treaty
,recoded_reinsurance_treaty
,policy_effective_month
,tenure
,claim_status
, date_diff(mon.date_knowledge,recoded_loss_date,DAY) as loss_dev_age_in_days 
, date_diff(mon.date_knowledge,recoded_loss_date,MONTH) as loss_dev_age_in_mos_by_acc_month_recoded 
, date_diff(date_first_notice_of_loss,recoded_loss_date, DAY) as reporting_lag_level_in_days_recoded
, trunc(date_diff(date_first_notice_of_loss,recoded_loss_date, DAY)/30.5,0) as reporting_within_num_month_lag_level_recoded
, date_diff(last_day(date_trunc(date_knowledge, MONTH),MONTH), date_add(last_day(date_trunc(date_of_loss, QUARTER),QUARTER), INTERVAL -3 MONTH), MONTH) as loss_dev_age_in_mos_by_quarter_recoded

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
,sum(case when claim_closed_no_total_payment is true then total_incurred_calc else 0 end) as incurred_total_net_recoveries_cumulative_on_closed_x_Closed_No_TOTAL_Pay
## Capped Loss

,least(100000, sum(loss_incurred_calc)) as capped_incurred_loss_net_recoveries_at_100k
,sum(loss_incurred_calc) - least(100000, sum(loss_incurred_calc)) as excess_incurred_loss_net_recoveries_at_100k

,sum(case when loss_incurred_calc = 0 then 0 else ((loss_paid-recoveries) / loss_incurred_calc) * least(100000,loss_incurred_calc) end) as capped_loss_paid_net_recoveries_at_100k
,sum(case when loss_incurred_calc = 0 then 0 else (loss_paid-recoveries) - (((loss_paid-recoveries) / loss_incurred_calc) * least(100000,loss_incurred_calc)) end) as excess_loss_paid_net_recoveries_at_100k

,sum(case when loss_incurred_calc = 0 then 0 else (loss_net_reserve / loss_incurred_calc) * least(100000,loss_incurred_calc) end) as capped_loss_case_reserves_at_100k
,sum(case when loss_incurred_calc = 0 then 0 else loss_net_reserve - ((loss_net_reserve / loss_incurred_calc) * least(100000,loss_incurred_calc)) end) as excess_loss_case_reserves_at_100k

,least(100000, sum(case when loss_incurred_calc = 0 then 0 else ((loss_paid-recoveries) / loss_incurred_calc * loss_incurred_calc) end)) as capped_loss_paid_net_recoveries_at_100k_gina --gina edit
--,sum(case when loss_incurred_calc = 0 then 0 else (loss_paid-recoveries) - (((loss_paid-recoveries) / loss_incurred_calc) * least(100000,loss_incurred_calc)) end) as excess_loss_paid_net_recoveries_at_100k

,least(100000, sum(case when loss_incurred_calc = 0 then 0 else ((loss_net_reserve / loss_incurred_calc) * loss_incurred_calc) end)) as capped_loss_case_reserves_at_100k_gina --gina edit
--,sum(case when loss_incurred_calc = 0 then 0 else loss_net_reserve - ((loss_net_reserve / loss_incurred_calc) * least(100000,loss_incurred_calc)) end) as excess_loss_case_reserves_at_100k

,sum(claim_count) as reported_claim_count
,sum(case when date_closed is null then claim_count else 0 end) as open_claim_count
,sum(case when date_closed is null then 0 else claim_count end) as closed_claim_count

,sum(case when claim_closed_no_total_payment is true then 0 else claim_count end) as reported_claim_count_x_Closed_No_TOTAL_Pay
,sum(case when claim_closed_no_loss_payment is true then 0 else claim_count end) as reported_claim_count_x_Closed_No_LOSS_Pay

,sum(case when date_closed is null then 0 when claim_closed_no_total_payment is true then 0 else claim_count end) as closed_claim_count_x_Closed_No_TOTAL_Pay
,sum(case when date_closed is null then 0 when claim_closed_no_loss_payment is true then 0 else claim_count end) as closed_claim_count_x_Closed_No_LOSS_Pay
,sum(case when date_closed is null then 0 when claim_closed_no_total_payment is true then claim_count else 0 end) as closed_claim_count_No_TOTAL_Pay
,sum(case when date_closed is null then 0 when claim_closed_no_loss_payment is true then 1 else 0 end) as closed_claim_count_No_LOSS_Pay

,sum(case when claim_closed_no_total_payment is true then 0 when total_incurred_calc >= 100000 then claim_count else 0 end) as reported_claim_count_x_Closed_No_TOTAL_Pay_excess_100k
,sum(case when claim_closed_no_loss_payment is true then 0 when total_incurred_calc >= 100000 then claim_count else 0 end) as reported_claim_count_x_Closed_No_LOSS_Pay_excess_100k
from claims_supp mon
where 1=1
and carrier <> 'canopius'
--and is_ebsl is false
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32
)
select 
*,
incurred_total_net_recoveries_cumulative*loss_dev_age_in_days/30.5 as dollar_wtd_age,
CONCAT(analysis_peril, '/', state, '/' ,accident_year_recoded, '/', extract(MONTH from accident_month_recoded)) as CAT_id
from x
where 1=1
and capped_incurred_loss_net_recoveries_at_100k > 100000
