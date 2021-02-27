with claims_supp as (
select mon.*
, case when cc.cat is true then 'Y'
    when cc.cat is false then 'N'
    when peril = 'wind' or peril = 'hail' then 'Y'
    when cat_code is not null then 'Y'
        else 'N' end as CAT
    , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
    , case when cc.recoded_loss_event is null then 'NA' else cc.recoded_loss_event end as recoded_loss_event
    , string_field_1 as new_peril_group
    , last_day(date_trunc(date_effective,MONTH),MONTH) as term_policy_effective_month
    , last_day(date_trunc(date_first_effective, MONTH),MONTH) as policy_effective_month
    , case when renewal_number > 0 then 'renewal' else 'new' end as tenure
,coalesce(recoverable_depreciation,0) as total_recoverable_depreciation
,coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) - coalesce(recoveries,0) as loss_incurred_calc
,coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) as expense_incurred_calc
,coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) + coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) - coalesce(recoveries,0) as total_incurred_calc
from dw_prod_extracts.ext_all_claims_combined mon
left join dbt_actuaries.claim_cat_coding_20210226  cc on (case when tbl_source = 'topa_tpa_claims' then trim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
left join (select policy_id, renewal_number from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2021-01-31') using(policy_id)
left join dbt_actuaries.claims_mappings_202012 map on mon.peril = map.string_field_0
left join (select policy_id, date_first_effective from dw_prod.dim_policies left join dw_prod.dim_policy_groups using (policy_group_id)) using (policy_id)
where (mon.date_knowledge = last_day(date_trunc(mon.date_knowledge, MONTH,MONTH)) or mon.date_knowledge in ('2021-02-26'))
)
,x as (
select 
-- mon.claim_number,
last_day(date_trunc(date_knowledge, MONTH),MONTH) as evaluation_date
-- ,last_day(date_trunc(date_knowledge, QUARTER),QUARTER) as calendar_quarter
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
,product
-- ,maturity
-- ,peril
,new_peril_group
,CAT
,reinsurance_treaty
,policy_effective_month
,tenure

## Total Loss

,sum(loss_paid-recoveries) as paid_loss_inc_recoveries_cumulative
,sum(expense_paid) as paid_expense_cumulative
,sum(total_paid) as paid_total_cumulative

,sum(loss_net_reserve) as outstanding_loss_case_reserves
,sum(expense_net_reserve) as outstanding_expense_case_reserves
,sum(total_net_reserve) as outstanding_total_case_reserves

,sum(recoveries) as total_recoveries

,sum(loss_incurred_calc) as incurred_loss_inc_recoveries_cumulative
,sum(expense_incurred_calc) as incurred_expense_cumulative
,sum(total_incurred_calc) as incurred_total_cumulative

,sum(total_recoverable_depreciation) as outstanding_recoverable_depreciation


## Capped Loss

,sum(least(100000,loss_incurred_calc)) as capped_incurred_loss_inc_recoveries_at_100k
,sum(loss_incurred_calc - least(100000,loss_incurred_calc)) as excess_incurred_loss_inc_recoveries_at_100k

,sum(case when loss_incurred_calc = 0 then 0 else ((loss_paid-recoveries) / loss_incurred_calc) * least(100000,loss_incurred_calc) end) as capped_loss_paid_inc_recoveries_at_100k
,sum(case when loss_incurred_calc = 0 then 0 else (loss_paid-recoveries) - (((loss_paid-recoveries) / loss_incurred_calc) * least(100000,loss_incurred_calc)) end) as excess_loss_paid_inc_recoveries_at_100k

,sum(case when loss_incurred_calc = 0 then 0 else (loss_net_reserve / loss_incurred_calc) * least(100000,loss_incurred_calc) end) as capped_loss_case_reserves_at_100k
,sum(case when loss_incurred_calc = 0 then 0 else loss_net_reserve - ((loss_net_reserve / loss_incurred_calc) * least(100000,loss_incurred_calc)) end) as excess_loss_case_reserves_at_100k

,count(distinct mon.claim_number) as reported_claim_count
,sum(case when date_closed is null then 1 else 0 end) as open_claim_count
,sum(case when date_closed is null then 0 else 1 end) as closed_claim_count

,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as reported_claim_count_x_Closed_No_TOTAL_Pay
,sum(case when claim_closed_no_loss_payment is true then 0 else 1 end) as reported_claim_count_x_Closed_No_LOSS_Pay

,sum(case when date_closed is null then 0 when claim_closed_no_total_payment is true then 0 else 1 end) as closed_claim_count_x_Closed_No_TOTAL_Pay
,sum(case when date_closed is null then 0 when claim_closed_no_loss_payment is true then 0 else 1 end) as closed_claim_count_x_Closed_No_LOSS_Pay
,sum(case when date_closed is null then 0 when claim_closed_no_total_payment is true then 1 else 0 end) as closed_claim_count_No_TOTAL_Pay
,sum(case when date_closed is null then 0 when claim_closed_no_loss_payment is true then 1 else 0 end) as closed_claim_count_No_LOSS_Pay

,sum(case when claim_closed_no_total_payment is true then 0 when total_incurred_calc >= 100000 then 1 else 0 end) as reported_claim_count_x_Closed_No_TOTAL_Pay_excess_100k
,sum(case when claim_closed_no_loss_payment is true then 0 when total_incurred_calc >= 100000 then 1 else 0 end) as reported_claim_count_x_Closed_No_LOSS_Pay_excess_100k

from claims_supp mon
where 1=1
-- and month_of_loss = '2020-04-01'
-- and month_knowledge = '2020-11-01'
and carrier <> 'canopius'
and is_ebsl is false
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
order by 1,2,3
)
select 
-- new_peril_group,
-- sum(incurred_loss_inc_recoveries_cumulative) as incurred_loss_inc_recoveries_cumulative,
-- sum(incurred_expense_cumulative) as incurred_expense_cumulative,
-- sum(incurred_total_cumulative) as incurred_total_cumulative
-- sum(total_recoveries) as total_recoveries,
-- sum(paid_loss_inc_recoveries_cumulative) as paid_loss_inc_recoveries_cumulative,
-- sum(paid_expense_cumulative) as paid_expense_cumulative,
-- sum(paid_total_cumulative) as paid_total_cumulative
-- sum(outstanding_loss_case_reserves) as outstanding_loss_case_reserves,
-- sum(capped_loss_paid_inc_recoveries_at_100k) as capped_loss_paid_inc_recoveries_at_100k,
-- sum(excess_loss_paid_inc_recoveries_at_100k) as excess_loss_paid_inc_recoveries_at_100k,
-- sum(capped_loss_case_reserves_at_100k) as capped_loss_case_reserves_at_100k,
-- sum(excess_loss_case_reserves_at_100k) as excess_loss_case_reserves_at_100k,
-- sum(capped_incurred_loss_at_100k) as capped_incurred_loss_at_100k,
-- sum(excess_incurred_loss_at_100k) as excess_incurred_loss_at_100k,
*
from x
where 1=1
-- and evaluation_date = '2020-12-31'
-- group by 1
-- order by 1
-- and evaluation_date = '2020-12-31'
-- and month_knowledge = '2020-11-01'
-- and accident_month = '2020-01-01'
-- and incremental_recoverable_depreciation < 0
-- and report_month = '2020-01-01'
-- and state = 'tx'
-- and peril = 'hail'
-- and product = 'ho3'
-- and open_at_end_of_prior_month_x_CNP_count > 0
-- and CAT = 'Y'
-- and claim_number = 'HIL-0403915-01-01'