with x as (
SELECT DISTINCT
       mon.*
      , loss_calculated_incurred AS Loss_Incurred
      , loss_calculated_net_reserve_corrected AS Loss_Net_Reserve
      , loss_calculated_total_net_paid AS Loss_Paid
      , loss_total_deductible_received AS Loss_Deductible
      , loss_total_recoverable_depreciation AS Loss_Recoverable_Depreciation
      , loss_recovery_subr_salv AS Loss_Recovered
      , expense_calculated_incurred AS Expense_Incurred
      , expense_calculated_net_reserve_corrected AS Expense_Net_Reserve
      , expense_calculated_total_net_paid AS Expense_Paid   
      , 0 AS Expense_Deductible
      , expense_total_recoverable_depreciation AS Expense_Recoverable_Depreciation
      , expense_recovery_subr_salv	AS Expense_Recovered
      , total_calculated_net_paid AS Total_Paid
      , total_net_reserves AS Total_Net_Reserve
      , total_incurred AS Total_Incurred
      , total_deductible_received AS Total_Deductibles
      , total_recovery_subr_salv AS Total_Recovery
      , Total_Recoverable_Depreciation
          , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
    , case when cc.recoded_loss_event is null then 'NA' else cc.recoded_loss_event end as recoded_loss_event
    , case when cc.Updated_Policy_Effective_Date is null then date_effective else cc.Updated_Policy_Effective_Date end as recoded_policy_effective_date
    , case when cc.Updated_Policy_Number is null then policy_number else cc.Updated_Policy_Number end as recoded_policy_number
    -- , case when cc.Recoded_carrier is null then carrier else cc.Recoded_carrier end as recoded_carrier
      ,loss_description
      ,damage_description
      ,dp.org_id as organization_id
  FROM dw_prod_extracts.ext_claims_inception_to_date mon
  left join (select claim_id, claim_number, loss_description, damage_description from dw_prod.dim_claims) fc using (claim_number)
  left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
  left join dbt_actuaries.cat_coding_w_loss_20210630 cc on mon.claim_number = cast(cc.claim_number as string)
--   left join dbt_actuaries.sf_claims_as_20210506_v1 sf on mon.claim_id = sf.pod_claim_id
  WHERE (date_knowledge = date_add(last_day(date_sub(date_knowledge,interval 1 week), week(sunday)), interval 1 day) or date_knowledge = '2021-07-11')
  and date_first_notice_of_loss <= '2021-07-11'
  and date_first_notice_of_loss >= '2021-01-01'
  and carrier <> 'canopius'
--   and is_ebsl is false
  )
select 
date_knowledge as Evaluation_Date,
claim_number as Claim_Number,
recoded_loss_date as Date_of_Loss,
date_first_notice_of_loss as Date_Reported,
state as State,
product as Product,
claim_status as Status,
recoded_loss_event as PCS_Event_Label,
coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) - coalesce(total_recovery,0) as Total_Reported_Loss_Net_of_Recoveries,
coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) as Total_Reported_Expense_Net_of_Recoveries,
coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) - coalesce(total_recovery,0) + coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) as Total_Reported_Loss_and_Expense_Net_of_Recoveries,
coalesce(loss_paid,0) - coalesce(total_recovery,0) as Total_Paid_Loss_Net_of_Recoveries,
coalesce(expense_paid,0) as Total_Paid_Expense,
coalesce(loss_paid,0) - coalesce(total_recovery,0) + coalesce(expense_paid,0) as Total_Paid_Loss_and_Expense_Net_of_Recoveries,
'' as placeholder_for_flag,
peril as Cause_of_Loss,
  from x
  where 1=1
  and peril in ('hail','wind')
--   and (recoded_loss_event in ('2115_direct', '2116_direct','2117_direct','2115_indirect','2116_indirect','2117_indirect','2115_indeterminate','2117_indeterminate')
--         or (peril = 'pipe_freezing' and date_first_notice_of_loss >= '2021-06-30'))