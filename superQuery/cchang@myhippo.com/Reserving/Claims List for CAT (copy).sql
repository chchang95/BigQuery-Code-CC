SELECT DISTINCT
    trim(mon.claim_number,'0'),
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
  FROM dw_prod_extracts.ext_all_claims_combined mon
  left join (select policy_id, case when organization_id is null then 0 else organization_id end as org_id, channel from dw_prod.dim_policies) dp on mon.policy_id = dp.policy_id
  left join (select policy_id, calculated_fields_non_cat_risk_class, calculated_fields_cat_risk_class, renewal_number from dw_prod_extracts.ext_policy_snapshots where date_snapshot = '2020-12-31') eps on eps.policy_id = mon.policy_id
  left join (select claim_number, loss_description, damage_description from dw_prod.dim_claims) fc on mon.claim_number = fc.claim_number
  left join (select date, last_day_of_quarter from dw_prod.utils_dates where date = date(last_day_of_quarter)) ud on mon.date_knowledge = date(ud.last_day_of_quarter)
  left join dbt_actuaries.cat_coding_w_loss_20201231 cc on (case when tbl_source = 'topa_tpa_claims' then trim(mon.claim_number,'0') else mon.claim_number end) = cast(cc.claim_number as string)
  WHERE 1=1
--   and date_knowledge <= '2021-01-31'
  and carrier <> 'canopius'
  and date_knowledge = '2020-12-31'
  and mon.claim_number = '000000107480'