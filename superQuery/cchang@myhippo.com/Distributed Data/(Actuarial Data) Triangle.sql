with claims_supp as (
select mon.*, cc.cat_ind as cat_indicator
from dw_prod_extracts.ext_claim_monthly mon
left join dw_staging_extracts.cc_cat_claim_coding_2020831 cc on mon.claim_number = cc.claim_number
where carrier <> 'canopius'
)
, aggregated as (
SELECT
-- mon.claim_number,
    mon.month_knowledge as as_of_month,
    mon.carrier,
    mon.state,
    -- mon.product,
    month_of_loss as accident_month,
    maturity,
    case when cat_indicator is true then 'Y'
    when cat_indicator is false then 'N'
    else 'Error' end as CAT
    -- ,reinsurance_treaty
    -- ,peril
    -- ,case when reinsurance_treaty = 'Spkr20_Classic' then 'Spkr19_GAP' else reinsurance_treaty end as original_treaty
    ,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as Reported_Claim_Count_Excl_Closed_No_Pay
    -- ,sum(case when date_close is null then 0 when claim_closed_no_total_payment is true then 0 else 1 end) as paid_claim_count_x_cnp
    -- ,sum(expense_calculated_incurred_inception_to_date) as ALAE_cumulative
    -- ,sum(loss_calculated_incurred_inception_to_date) as Indemnity_cumulative
    ,sum(total_incurred_inception_to_date) as Incurred_Loss_Cumulative
    -- ,sum(total_incurred_delta_this_month) as total_incurred_incremental
    -- ,sum(case when total_incurred_inception_to_date >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_catastrophe is true) then 0 else total_incurred_inception_to_date end) as small_NC_total_incurred_cumulative
    -- ,sum(case when total_incurred_inception_to_date >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_catastrophe is true) then total_incurred_inception_to_date else 0 end) as large_NC_total_incurred_cumulative
    ,sum(case when cat_indicator is false then 100000 else total_incurred_inception_to_date end) as Incurred_Loss_Capped_At_100k_NonCat_Only_Cumulative
    ,sum(case when cat_indicator is false then total_incurred_inception_to_date - 100000 else 0 end) as Incurred_Loss_Excess_of_100k_NonCat_Only_Cumulative
  FROM
    claims_supp mon
    -- left join (select claim_number, reinsurance_treaty from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = '2020-08-31') USING(claim_number)
  where is_ebsl is false
--   and cat_indicator = false
  and month_of_loss >= '2019-09-01'
  and mon.month_knowledge <= '2020-08-31'
  group by 1,2,3,4,5,6
  )
 select * from aggregated