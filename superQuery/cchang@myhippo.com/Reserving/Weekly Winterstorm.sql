with x as (
SELECT DISTINCT
    case when cc.updated_treaty is null then reinsurance_treaty else cc.updated_treaty end as recoded_reinsurance_treaty
      , policy_number AS Policy_Number
      , date_effective AS Inception_Date
      , product as Product
          , case when cc.Updated_Policy_Number is null then policy_number else cc.Updated_Policy_Number end as recoded_policy_number
      , date_expires AS Expiration_Date
      , mon.claim_number AS Claim_Number
      , date_of_loss AS original_loss_date
      , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
      , date_first_notice_of_loss AS Report_Date 
      , insured_name AS Insured_Name
      , street AS Loss_Location
      , city AS Loss_City
      , state AS Loss_State
      , zip_code AS Loss_Zip
      , claim_status AS Claim_Status 
      , peril AS Cause_Of_Loss
      , date_close AS Closed_Date
      --, '2019' AS Treaty_Year
    , case when cc.recoded_loss_event is null then 'NA' else cc.recoded_loss_event end as recoded_loss_event
      , case when cc.cat_ind is true then 'Y'
    when cc.cat_ind is false then 'N'
    when peril = 'wind' or peril = 'hail' then 'Y'
    when cat_code is not null then 'Y'
        else 'N' end as CAT
      , assigned_adjuster AS Adjuster_Name 
      , carrier AS Carrier
      , customer_email AS Customer_Email
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
      , case when peril = 'equipment_breakdown' or peril = 'service_line' then true
      else is_ebsl end as EBSL
      ,loss_description
      ,damage_description
      ,dp.org_id as organization_id
      ,date_knowledge
  FROM dw_prod_extracts.ext_claims_inception_to_date mon
  left join (select claim_id, claim_number, loss_description, damage_description from dw_prod.dim_claims) fc using (claim_number)
  left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
  left join dbt_actuaries.cat_coding_w_loss_20210430 cc on cc.claim_number = mon.claim_number
  WHERE 1=1 
--   and date_knowledge = date_trunc(date_knowledge,week)
  and date_knowledge = '2021-04-30'
--   and date_knowledge >= '2021-02-01'
  and carrier <> 'canopius'
--   and is_ebsl is false
  )
  select 
  date_knowledge,
  'Hippo' as ClaimsHandler
  ,lower(Carrier) as carrier
  ,Loss_State as Policy_State
  ,date_trunc(original_loss_date, MONTH) as original_accident_month
  ,date_trunc(recoded_loss_date, MONTH) as recoded_accident_month
  ,lower(Product) as Product
  ,policy_number as original_policy_number
  ,recoded_policy_number as recoded_policy_number
  ,Inception_Date
  ,Expiration_Date
  ,Claim_Number
  ,original_loss_date
  ,recoded_loss_date
  ,report_date
  ,loss_city
  ,loss_state
  ,loss_zip
  ,claim_status
  ,cause_of_loss
  ,closed_date
  ,CAT as CAT_indicator
,case when CAT = 'N' then null
    when (CAT = 'Y' and recoded_loss_event in ('2115_direct', '2116_direct','2117_direct','2115_indirect','2116_indirect','2117_indirect','2115_indeterminate','2117_indeterminate'))
        then '202102_winterstorm'
    when (CAT = 'Y' and recoded_loss_event not in ('2115_direct', '2116_direct','2117_direct','2115_indirect','2116_indirect','2117_indirect','2115_indeterminate','2117_indeterminate'))
        then 'non_winterstorm'
    end as cat_group  
    ,recoded_loss_event
    ,EBSL
  ,loss_paid as loss_paid_x_recoveries
  ,Loss_Net_Reserve
  ,expense_paid as expense_paid_x_recoveries
  ,expense_net_reserve
  ,Total_Recovery
  ,coalesce(loss_paid) + coalesce(loss_net_reserve) + coalesce(expense_paid) + coalesce(expense_net_Reserve) - coalesce(total_recovery) as total_incurred_inc_recoveries
  ,organization_id
  ,loss_description
  ,damage_description
  ,recoded_reinsurance_treaty
--   ,Adjuster_Name
--   ,Total_Recoverable_Depreciation
  from x
  where 1=1
--   and CAT = 'Y'
--   and report_date between '2021-04-01' and '2021-04-30'
  order by 1
--   and Loss_State = 'mi'
-- and loss_net_reserve > 0 and claim_status = 'closed'
-- and report_date >= '2021-01-31'