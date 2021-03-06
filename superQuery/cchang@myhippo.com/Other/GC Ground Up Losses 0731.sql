with x as (
SELECT DISTINCT
       reinsurance_treaty AS Treaty
      , policy_number AS Policy_Number
      , date_effective AS Inception_Date
      , product as Product
      , date_expires AS Expiration_Date
      , claim_number AS Claim_Number
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
      , loss_total_deductible_received AS Loss_Deductible_Received
      , loss_total_deductible_reserve as Loss_Deductible_Reserve
      , loss_total_gross_reserve
      , loss_total_recoverable_depreciation AS Loss_Recoverable_Depreciation
      , loss_total_non_recoverable_depreciation
      , loss_recovery_subr_salv AS Loss_Recovered
      , loss_total_recovery_overpayment
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
      , total_gross_reserves
      , case when peril = 'equipment_breakdown' or peril = 'service_line' then 'Y'
      else 'N' end as EBSL
      , case when peril = 'wind' or peril = 'hail' then 'Y'
      when cat_code is not null then 'Y'
      else 'N' end as CAT
      ,fc.*
      ,dp.org_id as organization_id
  FROM dw_prod_extracts.ext_claims_inception_to_date mon
  left join (select claim_id, loss_description, damage_description from dw_prod.fct_claims) fc using (claim_id)
  left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
  WHERE date_knowledge = '2020-07-31'
  and carrier <> 'Canopius'
  )
  , combined as (select 'Hippo' as ClaimsHandler
  ,lower(Carrier) as carrier
  ,Loss_State as Policy_State
  ,date_trunc(loss_date, MONTH) as accident_month
  ,lower(Product) as Product
  ,policy_number
  ,Inception_Date
  ,Expiration_Date
  ,Claim_Number
  ,loss_date
  ,report_date
  ,loss_city
  ,loss_state
  ,loss_zip
  ,claim_status
  ,cause_of_loss
  ,closed_date
  ,CAT as CAT_indicator
  ,'' as placeholder
  ,EBSL
  , Loss_Incurred
  ,loss_paid
  ,Loss_Net_Reserve
  ,Loss_Deductible_Received
  ,Loss_Deductible_Reserve
  ,loss_total_gross_reserve
  ,Loss_Recoverable_Depreciation
  ,loss_total_non_recoverable_depreciation
  , Loss_Recovered
  , loss_total_recovery_overpayment
  ,expense_paid
  ,expense_net_reserve
  ,Total_Recovery
  ,organization_id
  ,CAT_code as internal_CAT_code
  ,loss_description
  ,damage_description
--   ,Total_Recoverable_Depreciation
  from x
--   where ebsl = 'N'
)
, summary as (
select sum(loss_paid) as loss_paid, sum(Loss_Net_Reserve) as Loss_Net_Reserve, sum(Loss_Deductible_Received) as Loss_Deductible_Received, sum(Loss_Deductible_Reserve) as Loss_Deductible_Reserve
,sum(loss_total_gross_reserve) as loss_total_gross_reserve, sum(Loss_Recoverable_Depreciation) as Loss_Recoverable_Depreciation, sum(Loss_Recovered) as Loss_Recovered, sum(Loss_Incurred) as Loss_Incurred
, sum(loss_total_non_recoverable_depreciation) as loss_total_non_recoverable_depreciation
, sum(loss_total_recovery_overpayment) as loss_total_recovery_overpayment
from combined
)
select * from combined