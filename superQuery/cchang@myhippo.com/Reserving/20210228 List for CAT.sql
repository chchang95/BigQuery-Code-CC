with claims_supp as (
SELECT DISTINCT
    mon.*
--       , is_ebsl
    , case when cc.cat_ind is true then 'Y'
    when cc.cat_ind is false then 'N'
    when peril = 'wind' or peril = 'hail' then 'Y'
    when cat_code is not null then 'Y'
        else 'N' end as CAT
    , case when cc.recoded_loss_date is null then date_of_loss else cc.recoded_loss_date end as recoded_loss_date
    , case when cc.recoded_loss_event is null then 'NA' else cc.recoded_loss_event end as recoded_loss_event
      ,dp.org_id
      ,dp.channel
      , date_trunc(date_effective, MONTH) as term_effective_month
      , case when renewal_number = 0 then "New" else "Renewal" end as tenure
      , case when property_data_address_state = 'tx' and calculated_fields_cat_risk_class = 'referral' then 'cat referral' 
              when calculated_fields_non_cat_risk_class is null or date_effective <= '2020-05-01' then 'not_applicable'
              else calculated_fields_non_cat_risk_class end as rated_uw_action
    , loss_description
    , damage_description
  FROM dbt_actuaries.ext_all_claims_combined_20210228 mon
  left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id, channel from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
  left join (select policy_id, calculated_fields_non_cat_risk_class, calculated_fields_cat_risk_class, renewal_number from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2021-02-28') eps on eps.policy_id = mon.policy_id
  left join (select claim_number, loss_description, damage_description from dw_prod.dim_claims) fc on mon.claim_number = fc.claim_number
  left join (select date, last_day_of_quarter from dw_prod.utils_dates where date = date(last_day_of_quarter)) ud on mon.date_knowledge = date(ud.last_day_of_quarter)
  left join dbt_actuaries.cat_coding_w_loss_20210228 cc on (case when tbl_source = 'topa_tpa_claims' then ltrim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
  WHERE 1=1
--   and date_knowledge = '2021-02-28'
  and carrier <> 'canopius'
--   and last_day_of_quarter is not null
  )
  , aggregated as (
    select 
    date_knowledge,
        case when tbl_source = 'hippo_claims' then 'Hippo' 
        when tbl_source = 'topa_tpa_claims' then 'TPA'
        when tbl_source = 'spinnaker_tpa_claims' then 'TPA'
        else 'ERROR' end as ClaimsHandler
        ,lower(Carrier) as carrier
        ,property_data_address_state as Policy_State
        ,date_trunc(date_of_loss, MONTH) as accident_month_original
        ,lower(Product) as Product
        ,claims_policy_number
        ,date_effective
        ,date_expires
        ,Claim_Number
        ,date_of_loss as original_loss_date
        ,recoded_loss_date as recoded_loss_date
        ,recoded_loss_Event as recoded_loss_event
        ,date_first_notice_of_loss
        ,property_data_address_city
        ,property_data_address_state
        ,property_data_address_zip
        ,claim_status
        ,peril
        ,date_closed
        ,CAT as CAT_indicator
--         ,'' as placeholder
        ,is_ebsl
        ,loss_paid
        ,Loss_Net_Reserve
        ,expense_paid
        ,expense_net_reserve
        ,recoveries
        ,coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) + coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) - coalesce(recoveries,0) as total_incurred
--         ,CAT_code as internal_CAT_code
        ,org_id as organization_id
--         ,reinsurance_treaty
        ,channel
        ,tenure
--         ,term_effective_month
--         ,rated_uw_action
--         ,policy_id
        ,loss_description
        ,damage_description
    --   ,Total_Recoverable_Depreciation    
  from claims_supp
  where 1=1
--   and is_ebsl is false
--   and claims_policy_number = 'HMO-0345091-00'
--   and carrier = 'Topa'
--   and tbl_source = 'hippo_claims'
--   and tbl_source <> 'hippo_claims'
  order by 1
  )
  select * from aggregated
--   where claim_number = 'CCA-1148557-00-01'
--   order by 1

  