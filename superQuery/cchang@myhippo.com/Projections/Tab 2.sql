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
left join dbt_cchin.cat_coding_20201031 cc on mon.claim_number = cc.claim_number
where carrier <> 'canopius'
)
, aggregated as (
SELECT
-- mon.claim_number,
    mon.date_knowledge as as_of_month,
    policy_effective_month,
    mon.carrier,
    mon.property_data_address_state as state,
    mon.product,
    accident_month as accident_month,
    maturity,
    CAT
    ,reinsurance_treaty
    ,peril
    -- ,case when reinsurance_treaty = 'Spkr20_Classic' then 'Spkr19_GAP' else reinsurance_treaty end as original_treaty
    ,sum(loss_paid) as loss_paid
    ,sum(expense_paid) as expese_paid
    ,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as Reported_Claim_Count_Excl_Closed_No_Pay
    ,sum(expense_incurred_calc) as ALAE_cumulative
    ,sum(total_incurred_calc) as Incurred_Loss_and_ALAE_Cumulative
    ,sum(case when total_incurred_calc >= 100000 and CAT = 'N' 
        then 100000 else total_incurred_calc end) as Incurred_Loss_and_ALAE_Capped_At_100k_NonCat_Only_Cumulative
    ,sum(case when total_incurred_calc >= 100000 and CAT = 'N' 
        then total_incurred_calc - 100000 else 0 end) as Incurred_Loss_and_ALAE_Excess_of_100k_NonCat_Only_Cumulative
  FROM
    claims_supp mon
  where is_ebsl is false
  and mon.date_knowledge <= '2020-10-31'
  and product <> 'ho5'
  group by 1,2,3,4,5,6,7,8,9,10
  )
 select *
 from aggregated