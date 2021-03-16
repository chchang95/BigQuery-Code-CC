with claims_supp as (
select *
, case when cc.cat_ind is true then 'Y'
    when cc.cat_ind is false then 'N'
    when peril = 'wind' or peril = 'hail' then 'Y'
    when cat_code is not null then 'Y'
        else 'N' end as CAT
from dw_prod_extracts.ext_claims_inception_to_date cd
left join dbt_actuaries.cat_coding_w_loss_20210228_new cc on cd.claim_number = cc.claim_number
where carrier <> 'canopius'
)
, claims as (
select
policy_id
, date_knowledge
,sum(total_incurred) as total_incurred
,sum(case when CAT = 'N' then total_incurred else 0 end) as non_cat_incurred
,sum(case when CAT = 'Y' then total_incurred else 0 end) as cat_incurred
,sum(case when claim_closed_no_total_payment is false then 1 else 0 end) as total_claim_count_x_cnp
,sum(case when claim_closed_no_total_payment is false and CAT = 'N' then 1 else 0 end) as non_cat_claim_count_x_cnp
,sum(case when claim_closed_no_total_payment is false and CAT = 'Y' then 1 else 0 end) as cat_claim_count_x_cnp
,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then 100000 else total_incurred end) as capped_non_cat_incurred
,sum(case when CAT = 'Y' then 0 when total_incurred >= 100000 then total_incurred - 100000 else 0 end) as excess_non_cat_incurred
from claims_supp
where is_ebsl is false
group by 1,2
)
, one_month as (
select s1.policy_id, s1.policy_number, s1.state, s1.property_data_address_zip, s1.date_policy_effective, s1.date_snapshot
	, c1.date_knowledge, s1.earned_exposure, s1.earned_base, s1.earned_policy_fee
	, (s1.earned_base + s1.earned_total_optionals - s1.earned_optionals_equipment_breakdown - s1.earned_optionals_service_line) as earned_premium_x_EBSL
	, c1.total_incurred, c1.non_cat_incurred, c1.cat_incurred, c1.total_claim_count_x_cnp, c1.non_cat_claim_count_x_cnp
	, c1.cat_claim_count_x_cnp, c1.capped_non_cat_incurred, c1.excess_non_cat_incurred
	from dw_prod_extracts.ext_policy_snapshots s1
	left join claims c1 on s1.policy_id = c1.policy_id and least(date_add(s1.date_policy_effective, interval 1 month),current_date()-1) = c1.date_knowledge
	where s1.date_snapshot = least(date_add(s1.date_policy_effective, interval 1 month),current_date()-1)
)
-- and c1.date_knowledge is not null
, two_month as (
select s2.policy_id, s2.policy_number, s2.state, s2.property_data_address_zip, s2.date_policy_effective, s2.date_snapshot
	, c2.date_knowledge, s2.earned_exposure, s2.earned_base, s2.earned_policy_fee
	, (s2.earned_base + s2.earned_total_optionals - s2.earned_optionals_equipment_breakdown - s2.earned_optionals_service_line) as earned_premium_x_EBSL
	, c2.total_incurred, c2.non_cat_incurred, c2.cat_incurred, c2.total_claim_count_x_cnp, c2.non_cat_claim_count_x_cnp
	, c2.cat_claim_count_x_cnp, c2.capped_non_cat_incurred, c2.excess_non_cat_incurred
	from dw_prod_extracts.ext_policy_snapshots s2
	left join claims c2 on s2.policy_id = c2.policy_id and least(date_add(s2.date_policy_effective, interval 2 month),current_date()-1) = c2.date_knowledge
	where s2.date_snapshot = least(date_add(s2.date_policy_effective, interval 2 month),current_date()-1)
)
, final as (select one.policy_id, one.policy_number, one.date_policy_effective, one.state
, one.date_snapshot as one_month_snapshot
, one.earned_exposure as one_month_earned_exposure
, one.earned_policy_fee as one_month_earned_policy_fee
, one.earned_premium_x_EBSL as one_month_earned_premium_x_EBSL
, one.total_incurred as one_month_total_incurred
, one.non_cat_incurred as one_month_non_cat_incurred
, one.total_claim_count_x_cnp as one_month_total_claim_count_x_cnp
, one.non_cat_claim_count_x_cnp as one_month_non_cat_claim_count_x_cnp
, two.date_snapshot as two_month_snapshot
, two.earned_exposure as two_month_earned_exposure
, two.earned_policy_fee as two_month_earned_policy_fee
, two.earned_premium_x_EBSL as two_month_earned_premium_x_EBSL
, two.total_incurred as two_month_total_incurred
, two.non_cat_incurred as two_month_non_cat_incurred
, two.total_claim_count_x_cnp as two_month_total_claim_count_x_cnp
, two.non_cat_claim_count_x_cnp as two_month_non_cat_claim_count_x_cnp
from one_month one left join two_month two on one.policy_id = two.policy_id)
select date_policy_effective, sum(one_month_earned_exposure), sum(one_month_non_cat_claim_count_x_cnp), sum(two_month_earned_exposure), sum(two_month_non_cat_claim_count_x_cnp)
from final
where date_policy_effective >= '2021-01-01'
group by 1
order by 1