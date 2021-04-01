with claims_supp as (
select 
mon.*
,last_day(date_of_loss, WEEK) as accident_week
,last_day(date_of_loss, MONTH) as accident_month
,last_day(date_of_loss, QUARTER) as accident_quarter
,last_day(date_first_notice_of_loss, WEEK) as report_week
,last_day(date_first_notice_of_loss, MONTH) as report_month
,last_day(date_first_notice_of_loss,QUARTER) as report_quarter
, string_field_1 as peril_group
, case when string_field_1 = 'Fire_Smoke' or string_field_1 = 'Water' or string_field_1 = 'Liability' or string_field_1 = 'Wind' or string_field_1 = 'Hail' or string_field_1 = 'Hurricane' then string_field_1 else 'Other' end as peril_group_grouped
, case when cc.cat_ind is true then 'Y'
    when cc.cat_ind is false then 'N'
    when peril = 'wind' or peril = 'hail' or peril = 'hurricane' then 'Y'
    when cat_code is not null then 'Y'
        else 'N' end as CAT
, last_day(date_trunc(date_effective,MONTH),MONTH) as term_policy_effective_month
, last_day(date_trunc(date_first_effective, MONTH),MONTH) as policy_effective_month
, case when renewal_number > 0 then 'renewal' else 'new' end as tenure
from dw_prod_extracts.ext_claims_inception_to_date mon
left join dbt_actuaries.cat_coding_w_loss_20210228_new cc on mon.claim_number = cc.claim_number
left join dbt_actuaries.claims_mappings_202012 map on mon.peril = map.string_field_0
left join (select policy_id, date_first_effective from dw_prod.dim_policies left join dw_prod.dim_policy_groups using (policy_group_id)) using (policy_id)
left join (select policy_id, renewal_number from dw_prod_extracts.ext_policy_snapshots where date_snapshot = (select max(date_snapshot) from dw_prod_extracts.ext_claims_inception_to_date)) using(policy_id)
where date_knowledge = (select max(date_knowledge) from dw_prod_extracts.ext_claims_inception_to_date)
)
,claims as (
select date_knowledge
, claim_id

, sum(total_incurred) as total_incurred
, sum(total_calculated_net_paid) as total_paid
, sum(total_net_reserves) as total_net_reserves

, sum(loss_calculated_incurred) as loss_incurred
, sum(loss_calculated_total_net_paid) as loss_paid
, sum(loss_calculated_net_reserve_corrected) as loss_net_reserves

, sum(expense_calculated_incurred) as expense_incurred
, sum(expense_total_payment_to_insured) as expense_paid
, sum(expense_calculated_net_reserve_corrected) as expense_net_reserves

, sum(1) as reported_claim_count
, sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as reported_claim_count_x_CNP
, sum(case when claim_status = 'open' then 0 when claim_closed_no_total_payment is true then 0 else 1 end) as closed_claim_count_x_CNP

from dw_prod_extracts.ext_claims_inception_to_date mon
left join dbt_actuaries.cat_coding_w_loss_20210228 cc on mon.claim_number = cc.claim_number
where date_knowledge >= '2019-09-01'
and date_knowledge = last_day(date_knowledge,MONTH)
and carrier <> 'canopius'
group by 1, 2
)
, agg_claims as (
select date_knowledge, claim_id,

case when lag(total_incurred) over(partition by claim_id order by date_knowledge) is null then total_incurred else total_incurred - lag(total_incurred) over(partition by claim_id order by date_knowledge) end as total_incurred_change,
case when lag(total_paid) over(partition by claim_id order by date_knowledge) is null then total_paid else total_paid - lag(total_paid) over(partition by claim_id order by date_knowledge) end as total_paid_change,
case when lag(total_net_reserves) over(partition by claim_id order by date_knowledge) is null then total_net_reserves else total_net_reserves - lag(total_net_reserves) over(partition by claim_id order by date_knowledge) end as total_net_reserves_change,
total_net_reserves as outstanding_total_net_reserves,

case when lag(loss_incurred) over(partition by claim_id order by date_knowledge) is null then loss_incurred else loss_incurred - lag(loss_incurred) over(partition by claim_id order by date_knowledge) end as loss_incurred_change,
case when lag(loss_paid) over(partition by claim_id order by date_knowledge) is null then loss_paid else loss_paid - lag(loss_paid) over(partition by claim_id order by date_knowledge) end as loss_paid_change,
case when lag(loss_net_reserves) over(partition by claim_id order by date_knowledge) is null then loss_net_reserves else loss_net_reserves - lag(loss_net_reserves) over(partition by claim_id order by date_knowledge) end as loss_net_reserves_change,
loss_net_reserves as outstanding_loss_net_reserves,

case when lag(expense_incurred) over(partition by claim_id order by date_knowledge) is null then expense_incurred else expense_incurred - lag(expense_incurred) over(partition by claim_id order by date_knowledge) end as expense_incurred_change,
case when lag(expense_paid) over(partition by claim_id order by date_knowledge) is null then expense_paid else expense_paid - lag(expense_paid) over(partition by claim_id order by date_knowledge) end as expense_paid_change,
case when lag(expense_net_reserves) over(partition by claim_id order by date_knowledge) is null then expense_net_reserves else expense_net_reserves - lag(expense_net_reserves) over(partition by claim_id order by date_knowledge) end as expense_net_reserves_change,
expense_net_reserves as outstanding_expense_net_reserves,

case when lag(reported_claim_count_x_CNP) over(partition by claim_id order by date_knowledge) is null then reported_claim_count_x_CNP else reported_claim_count_x_CNP - lag(reported_claim_count_x_CNP) over(partition by claim_id order by date_knowledge) end as reported_claim_count_x_CNP_change,
case when lag(closed_claim_count_x_CNP) over(partition by claim_id order by date_knowledge) is null then closed_claim_count_x_CNP else closed_claim_count_x_CNP - lag(closed_claim_count_x_CNP) over(partition by claim_id order by date_knowledge) end as closed_claim_count_x_CNP_change,

reported_claim_count as cumulative_reported_claim_count,
reported_claim_count_x_CNP as cumulative_reported_claim_count_x_CNP,
closed_claim_count_x_CNP as cumulative_closed_claim_count_x_CNP

from claims
order by 1
)
, premium as (
select date_snapshot, carrier
, sum(earned_base + earned_total_optionals -earned_optionals_equipment_breakdown -earned_optionals_service_line) as total_earned_x_ebsl_x_pol_fees
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot >= '2019-09-01'
and date_snapshot = date_trunc(date_snapshot,MONTH)
and carrier <> 'canopius'
group by 1, 2
)
, agg_premium as (
select date_snapshot, carrier , 
total_earned_x_ebsl_x_pol_fees - lag(total_earned_x_ebsl_x_pol_fees) over(partition by carrier order by date_snapshot) as total_earned_x_ebsl_x_pol_fees_change,
from premium
order by 1
)
select ac.date_knowledge, carrier, state, product, CAT, peril_group, peril_group_grouped, accident_week, accident_month, accident_quarter, report_week, report_month, report_quarter
, term_policy_effective_month, policy_effective_month, tenure
, case when cs.date_first_notice_of_loss >= date_sub(ac.date_knowledge, INTERVAL 7 DAY) then 'New_Claim' else 'Existing_Claim' end as claim_type_week
, case when cs.date_first_notice_of_loss >= date_sub(ac.date_knowledge, INTERVAL 1 DAY) then 'New_Claim' else 'Existing_Claim' end as claim_type_day
, case when cs.date_first_notice_of_loss >= date_sub(ac.date_knowledge, INTERVAL 1 MONTH) then 'New_Claim' else 'Existing_Claim' end as claim_type_month
, case when cs.date_first_notice_of_loss >= date_sub(ac.date_knowledge, INTERVAL 1 QUARTER) then 'New_Claim' else 'Existing_Claim' end as claim_type_quarter
, case when cs.date_first_notice_of_loss >= date_sub(ac.date_knowledge, INTERVAL 1 YEAR) then 'New_Claim' else 'Existing_Claim' end as claim_type_year
, cast(sum(ac.total_incurred_change) as numeric) as total_incurred_change
, sum(ac.total_paid_change) as total_paid_change
, sum(ac.total_net_reserves_change) as total_net_reserves_change
, sum(ac.outstanding_total_net_reserves) as outstanding_total_net_reserves
, sum(ac.loss_incurred_change) as loss_incurred_change
, sum(ac.loss_paid_change) as loss_paid_change
, sum(ac.loss_net_reserves_change) as loss_net_reserves_change
, sum(ac.outstanding_loss_net_reserves) as outstanding_loss_net_reserves
, sum(ac.expense_incurred_change) as expense_incurred_change
, sum(ac.expense_paid_change) as expense_paid_change
, sum(ac.expense_net_reserves_change) as expense_net_reserves_change
, sum(ac.outstanding_expense_net_reserves) as outstanding_expense_net_reserves
, sum(ac.reported_claim_count_x_CNP_change) as reported_claim_count_x_CNP_change
, sum(ac.closed_claim_count_x_CNP_change) as closed_claim_count_x_CNP_change
, sum(ac.cumulative_reported_claim_count) as cumulative_reported_claim_count
, sum(ac.cumulative_reported_claim_count_x_CNP) as cumulative_reported_claim_count_x_CNP
, sum(ac.cumulative_closed_claim_count_x_CNP) as cumulative_closed_claim_count_x_CNP

from agg_claims ac
left join claims_supp cs on ac.claim_id = cs.claim_id
where ac.date_knowledge <> '2019-09-31'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
order by 1