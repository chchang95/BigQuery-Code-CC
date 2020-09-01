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
    ,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as claim_count_x_cnp
    ,sum(case when date_closed is null then 0 when claim_closed_no_total_payment is true then 0 else 1 end) as paid_claim_count_x_cnp
    ,sum(expense_incurred) as ALAE_cumulative
    ,sum(loss_incurred) as Indemnity_cumulative
    ,sum(recoveries) as Recoveries_cumulative
    ,sum(total_incurred) as total_incurred_cumulative
    ,sum(recoverable_depreciation) as recoverable_depreciation_cumulative
    ,sum(case when total_incurred >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_cat is true) then 100000 else total_incurred end) as capped_NC_total_incurred_cumulative
    ,sum(case when total_incurred >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_cat is true) then total_incurred - 100000 else 0 end) as excess_NC_total_incurred_cumulative
  FROM
    dw_prod_extracts.ext_all_claims_combined mon
    -- left join (select claim_number, reinsurance_treaty from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = @as_of) USING(claim_number)
  where is_ebsl is false
  group by 1,2,3,4,5,6,7,8,9,10,11,12