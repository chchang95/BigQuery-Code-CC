with x as (
SELECT DISTINCT
       reinsurance_treaty AS Treaty
      , policy_number AS Policy_Number
      , date_effective AS Inception_Date
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
      ,policy_group_id
      ,dp.policy_id
  FROM dw_prod_extracts.ext_claims_inception_to_date mon
  left join (select claim_id, claim_number, loss_description, damage_description from dw_prod.dim_claims) fc using (claim_number)
  left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
  left join (select policy_id, date_first_effective, policy_group_id from dw_prod.dim_policies left join dw_prod.dim_policy_groups using (policy_group_id)) dpg on mon.policy_id = dpg.policy_id
  WHERE date_knowledge = '2021-03-10'
  and carrier <> 'canopius'
--   and is_ebsl is false
  )
, policies as (
  
  select policy_group_id, max(date_first_effective) as first_term_eff, max(date_effective) as last_term_eff, max(renewal_number) as max_term
  from dw_prod.dim_policies 
  left join dw_prod.dim_policy_groups using (policy_group_id)
group by 1
)
select policy_id, policy_number, claim_number, treaty, loss_date, report_date, Inception_Date, date_expires, first_term_eff, last_term_eff, max_term
from x
left join policies using(policy_group_id)