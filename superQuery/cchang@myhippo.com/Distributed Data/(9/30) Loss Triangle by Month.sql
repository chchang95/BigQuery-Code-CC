with claims_supp as (
select mon.*
, case when cc.cat_ind is true then 'Y'
    when cc.cat_ind is false then 'N'
    when peril = 'wind' or peril = 'hail' then 'Y'
    when cat_code is not null then 'Y'
        else 'N' end as CAT
, date_trunc(date_effective, MONTH) as policy_effective_month
, coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) + coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) - coalesce(recoveries,0) as total_incurred_calc
, coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) as expense_incurred_calc
from dw_prod_extracts.ext_all_claims_combined mon
left join dbt_cchin.cat_coding_20200930 cc on mon.claim_number = cc.claim_number
where carrier <> 'canopius'
)
, aggregated as (
SELECT
-- mon.claim_number,
    mon.date_knowledge as evaluation_month,
    policy_effective_month,
    -- mon.carrier,
    -- mon.property_data_address_state as state,
    -- mon.product,
    accident_month as accident_month,
    maturity,
    CAT
    -- ,reinsurance_treaty_property
    -- ,peril
    -- ,case when reinsurance_treaty = 'Spkr20_Classic' then 'Spkr19_GAP' else reinsurance_treaty end as original_treaty
    ,sum(loss_paid) as Cumulative_Loss_Paid
    ,sum(expense_paid) as Cumulative_ALAE_Paid
    ,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as Reported_Claim_Count_Excl_Closed_No_Pay
    -- ,sum(case when date_close is null then 0 when claim_closed_no_total_payment is true then 0 else 1 end) as paid_claim_count_x_cnp
    ,sum(expense_incurred_calc) as Cumulative_ALAE_Incurred
    -- ,sum(loss_calculated_incurred_inception_to_date) as Indemnity_cumulative
    ,sum(total_incurred_calc) as Cumulative_Loss_Incurred
    -- ,sum(total_incurred_delta_this_month) as total_incurred_incremental
    -- ,sum(case when total_incurred_inception_to_date >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_catastrophe is true) then 0 else total_incurred_inception_to_date end) as small_NC_total_incurred_cumulative
    -- ,sum(case when total_incurred_inception_to_date >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_catastrophe is true) then total_incurred_inception_to_date else 0 end) as large_NC_total_incurred_cumulative
    ,sum(case when total_incurred_calc >= 100000 and CAT = 'N' 
        then 100000 else total_incurred_calc end) as Cumulative_Incurred_Loss_and_ALAE_Capped_At_100k_NonCat_Only
    ,sum(case when total_incurred_calc >= 100000 and CAT = 'N' 
        then total_incurred_calc - 100000 else 0 end) as Cumulative_Incurred_Loss_and_ALAE_Excess_of_100k_NonCat_Only
  FROM
    claims_supp mon
    -- left join (select claim_number, reinsurance_treaty from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = '2020-08-31') USING(claim_number)
  where is_ebsl is false
--   and cat_indicator = false
  and mon.date_knowledge <= '2020-09-30'
  and product <> 'ho5'
  group by 1,2,3,4,5
  )
 select *
 from aggregated