with claims_supp as (
select distinct
    mon.*
      , case when mon.is_cat_override is true then 'Y'
         when mon.is_cat_override is false then 'N'
        when peril = 'wind' or peril = 'hail' then 'Y'
        else 'N' end as CAT
      ,eps.region_code
      ,dp.channel
      , case when renewal_number = 0 then "New" else "Renewal" end as tenure
      , case when calculated_fields_cat_risk_class = 'referral' then 'cat referral'
             when calculated_fields_non_cat_risk_class is null or date_effective <= '2020-05-01' then 'not_applicable'
              else calculated_fields_non_cat_risk_class end as rated_uw_action
    , loss_description
    , damage_description
  from dw_prod_extracts.ext_all_claims_combined mon
  left join (select policy_id, channel from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
  left join (select policy_id, region_code, calculated_fields_non_cat_risk_class, calculated_fields_cat_risk_class, renewal_number from dw_prod_extracts.ext_policy_snapshots where date_snapshot = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) eps on eps.policy_id = mon.policy_id
  left join (select claim_number, loss_description, damage_description from dw_prod.dim_claims) fc on mon.claim_number = fc.claim_number
  where carrier <> 'canopius'
  and property_data_address_state = 'tx'
  and peril_group = 'Water'
  ),
  
  claim_cov as (
  select 
    claim_number,
    date_knowledge,
    coverage_a_gross_reserve,
    coverage_a_payment_to_insured,	
    coverage_a_deductible_received,	
    coverage_a_recoverable_depreciation,
    coverage_a_recovery_overpayment,
    coverage_b_gross_reserve,
    coverage_b_payment_to_insured,
    coverage_b_deductible_received,
    coverage_b_recoverable_depreciation,
    coverage_b_recovery_overpayment,
    coverage_c_gross_reserve,
    coverage_c_payment_to_insured,
    coverage_c_deductible_received,
    coverage_c_recoverable_depreciation,
    coverage_c_recovery_overpayment,
    coverage_d_gross_reserve,
    coverage_d_payment_to_insured,
    coverage_d_deductible_received,
    coverage_d_recoverable_depreciation,
    coverage_d_recovery_overpayment,
    coverage_ebsl_gross_reserve,
    coverage_ebsl_payment_to_insured,
    coverage_ebsl_deductible_received,
    coverage_ebsl_recoverable_depreciation,
    coverage_ebsl_recovery_overpayment,
    coverage_scheduled_personal_property_gross_reserve,
    coverage_scheduled_personal_property_payment_to_insured,
    coverage_scheduled_personal_property_deductible_received,
    coverage_scheduled_personal_property_recoverable_depreciation,
    coverage_scheduled_personal_property_recovery_overpayment,
    coverage_other_gross_reserve,
    coverage_other_payment_to_insured,
    coverage_other_deductible_received,
    coverage_other_recoverable_depreciation,
    coverage_other_recovery_overpayment
from dw_prod_extracts.ext_claims_inception_to_date)

select
    claims_supp.date_knowledge
    ,case when tbl_source = 'hippo_claims' then 'Hippo' 
    when tbl_source = 'topa_tpa_claims' then 'TPA'
    when tbl_source = 'spinnaker_tpa_claims' then 'TPA'
    else 'ERROR' end as claims_handler
    ,claims_supp.Claim_Number
    ,claims_policy_number as policy_number
    ,lower(Carrier) as carrier
    ,lower(Product) as product
    ,reinsurance_treaty
    ,channel
    ,organization_id
    ,tenure
    ,rated_uw_action
    ,coverage_a
    ,coverage_b
    ,coverage_c
    ,coverage_d
    ,date_effective as policy_date_effective
    ,date_expires as policy_date_expires
    ,property_data_address_state as state
    ,property_data_address_city
    ,property_data_address_state
    ,property_data_address_zip
    ,region_code
    ,last_day(date_of_loss, MONTH) as accident_month
    ,last_day(date_first_notice_of_loss, MONTH) as report_month
    ,date_of_loss as accident_date
    ,date_first_notice_of_loss as reported_date
    ,claim_status
    ,case when CAT = "N" and date_diff(date_of_loss, date_effective, day) <= 30 then 1 else 0 end AS non_cat_within_30_days_ind
    ,case when CAT = "N" and total_incurred >= 100000 then 1 else 0 end AS non_cat_large_loss_ind
    ,peril
    ,peril_group
    ,date_closed
    ,CAT as CAT_indicator
    ,is_ebsl
    ,claim_count
    ,claim_closed_no_loss_payment
    ,claim_closed_no_total_payment
    ,case when claim_closed_no_total_payment is false then claim_count else 0 end as reported_total_claim_count_x_cnp
    ,loss_description
    ,damage_description
    ,loss_paid
    ,Loss_Net_Reserve
    ,expense_paid
    ,expense_net_reserve
    ,recoveries
    ,coalesce(loss_paid,0) + coalesce(loss_net_reserve,0) + coalesce(expense_paid,0) + coalesce(expense_net_reserve,0) - coalesce(recoveries,0) as total_incurred
    ,claim_cov.*
from claims_supp
    left join claim_cov on claims_supp.claim_number=claim_cov.claim_number and claims_supp.date_knowledge = claim_cov.date_knowledge
where 1=1
and is_ebsl is false

