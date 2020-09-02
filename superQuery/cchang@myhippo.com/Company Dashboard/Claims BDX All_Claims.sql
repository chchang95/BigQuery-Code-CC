with claims_supp as (
SELECT DISTINCT
    *
--       , is_ebsl
      , case when peril = 'wind' or peril = 'hail' then 'Y'
      when cat_code is not null then 'Y'
      when is_CAT is true then 'Y'
      else 'N' end as CAT
    --   ,dp.org_id as organization_id
  FROM dw_prod_extracts.ext_all_claims_combined mon
  left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
  WHERE date_report_period_end = '2020-08-31'
  and carrier <> 'Canopius'
  )
    select 
        case when tbl_source = 'hippo_claims' then 'Hippo' 
        when tbl_source = 'topa_tpa_claims' then 'TPA'
        when tbl_source = 'spinnaker_tpa_claims' then 'TPA'
        else 'ERROR' end as ClaimsHandler
        ,lower(Carrier) as carrier
        ,property_data_address_state as Policy_State
        ,date_trunc(date_of_loss, MONTH) as accident_month
        ,lower(Product) as Product
        ,claims_policy_number
        ,date_effective
        ,date_expires
        ,Claim_Number
        ,date_of_loss
        ,date_first_notice_of_loss
        ,property_data_address_city
        ,property_data_address_state
        ,property_data_address_zip
        ,claim_status
        ,peril
        ,date_closed
        ,CAT as CAT_indicator
        ,'' as placeholder
        ,is_ebsl
        ,loss_paid
        ,Loss_Net_Reserve
        ,expense_paid
        ,expense_net_reserve
        ,recoveries
        ,org_id as organization_id
        ,CAT_code as internal_CAT_code
    --   ,Total_Recoverable_Depreciation    
  from claims_supp
  where 1=1
--   and is_ebsl is false
  and carrier = 'Topa'
  and tbl_source = 'hippo_claims'
--   and tbl_source <> 'hippo_claims'
  order by 1
  
--   select * from dw_prod_extracts.ext_claims_inception_to_date mon
--   where claim_number = 'HCA-1029622-00-01'