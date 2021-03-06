with x as (
SELECT DISTINCT
       reinsurance_treaty AS Treaty
      , policy_number AS Policy_Number
      , date_effective AS Inception_Date
      , mon.claim_id
      , product as Product
      , date_expires AS Expiration_Date
      , mon.claim_number AS Claim_Number
      , date_of_loss AS Loss_Date 
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
      , cat_code AS Cat_Code
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
      , case when peril = 'equipment_breakdown' or peril = 'service_line' then 'Y'
      else 'N' end as EBSL
      , case when peril = 'wind' or peril = 'hail' then 'Y'
      when cat_code is not null then 'Y'
      else 'N' end as CAT
      ,loss_description
      ,damage_description
      ,dp.org_id as organization_id
    , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
    , case when cc.recoded_loss_event is null then 'NA' else cc.recoded_loss_event end as recoded_loss_event
    , case when cc.Updated_Policy_Effective_Date is null then date_effective else cc.Updated_Policy_Effective_Date end as recoded_policy_effective_date
    , case when cc.Updated_Policy_Number is null then policy_number else cc.Updated_Policy_Number end as recoded_policy_number
  FROM dw_prod_extracts.ext_claims_inception_to_date mon
  left join (select claim_id, claim_number, loss_description, damage_description from dw_prod.dim_claims) fc using (claim_number)
  left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
  left join dbt_actuaries.cat_coding_w_loss_20210524 cc on mon.claim_number = cast(cc.claim_number as string)
  WHERE date_knowledge = '2021-07-07'
  and carrier <> 'canopius'
--   and is_ebsl is false
  )
  select 'Hippo' as ClaimsHandler
  ,lower(Carrier) as carrier
  ,Loss_State as Policy_State
  ,date_trunc(loss_date, MONTH) as accident_month
  ,lower(Product) as Product
  ,policy_number
  ,recoded_policy_number
  ,Inception_Date
  ,Expiration_Date
  ,Claim_Number
  ,loss_date
  ,recoded_loss_date
  ,report_date
  ,loss_city
  ,loss_state
  ,loss_zip
  ,claim_status
  ,cause_of_loss
  ,closed_date
  ,recoded_loss_event
--   ,CAT as CAT_indicator
--   ,'' as placeholder
  ,EBSL
  ,loss_paid
  ,Loss_Net_Reserve
  ,expense_paid
  ,expense_net_reserve
  ,Total_Recovery
  ,claim_id
--   ,organization_id
  ,CAT_code as internal_CAT_code
  ,loss_description
  ,damage_description
--   ,treaty
--   ,Adjuster_Name
--   ,Total_Recoverable_Depreciation
  from x
  where 1=1
  and cat_code is not null
--   and report_date >= '2021-03-01'
--   and cause_of_loss in ('wind','hail')
--   and carrier = 'spinnaker'
--   and report_date >= '2021-03-01'
--   and Loss_State = 'mi'
-- and loss_net_reserve > 0 and claim_status = 'closed'
-- and report_date >= '2021-01-31'