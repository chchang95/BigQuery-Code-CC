with check as (
SELECT
    date_report_period_end as evaluation_month,
    mon.carrier,
    mon.property_data_address_state as state,
    mon.product,
    accident_month,
    date_trunc(date_first_notice_of_loss, MONTH) as report_month,
    maturity,
    case when mon.peril = 'wind' or mon.peril = 'hail' then 'Y'
      when is_cat is true then 'Y'
      else 'N' end as CAT
    ,reinsurance_treaty
    ,peril
    ,peril_group
    -- ,case when phc.note is null then 'Not_Partner' else phc.note end as Partner_Handling
    ,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as claim_count_x_cnp
    ,sum(case when date_closed is null then 0 when claim_closed_no_total_payment is true then 0 else 1 end) as paid_claim_count_x_cnp
    ,sum(expense_incurred) as ALAE_cumulative
    ,sum(loss_incurred) as Indemnity_cumulative
    ,sum(recoveries) as Recoveries_cumulative
    ,sum(loss_paid +Loss_Net_Reserve + expense_paid + expense_net_reserve - recoveries) as incurred_2
    ,sum(total_incurred) as total_incurred_cumulative
    ,sum(coalesce(recoverable_depreciation,0)) as recoverable_depreciation_cumulative
    ,sum(case when total_incurred >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_cat is true) then 100000 else total_incurred end) as capped_NC_total_incurred_cumulative
    ,sum(case when total_incurred >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_cat is true) then total_incurred - 100000 else 0 end) as excess_NC_total_incurred_cumulative
  FROM
    dw_prod_extracts.ext_all_claims_combined mon
    left join (select distinct claim_number, note from dw_staging.chin_partner_handled_claims) phc on phc.claim_number = mon.claim_number 
    -- left join (select claim_number, reinsurance_treaty from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = @as_of) USING(claim_number)
  where is_ebsl is false
  group by 1,2,3,4,5,6,7,8,9,10,11
  )
  select accident_month,
  sum(incurred_2) as incurred_2,
  sum(total_incurred_cumulative) as total_incurred
  from check
  where evaluation_month = '2020-07-31'
  and carrier <> 'Canopius'
  group by 1
  order by 1