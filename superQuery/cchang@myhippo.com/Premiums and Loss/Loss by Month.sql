  SELECT
--     mon.claim_number,
    mon.month_knowledge,
    mon.carrier,
    mon.state,
    mon.product,
    month_of_loss,
    maturity,
    case when mon.peril = 'wind' or mon.peril = 'hail' then 'Y'
      when is_catastrophe is true then 'Y'
      else 'N' end as CAT
    ,reinsurance_treaty
    -- ,case when reinsurance_treaty = 'Spkr20_Classic' then 'Spkr19_GAP' else reinsurance_treaty end as original_treaty
    ,mon.peril
    ,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as claim_count_x_cnp
    ,sum(total_incurred_inception_to_date) as total_incurred_cumulative
    ,sum(total_incurred_delta_this_month) as total_incurred_incremental
    ,sum(case when total_incurred_inception_to_date >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_catastrophe is true) then 100000 else total_incurred_inception_to_date end) as capped_NC_total_incurred_cumulative
    ,sum(case when total_incurred_inception_to_date >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_catastrophe is true) then total_incurred_inception_to_date - 100000 else 0 end) as excess_NC_total_incurred_cumulative
  FROM
    dw_prod_extracts.ext_claim_monthly mon
    left join (select claim_number, reinsurance_treaty from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = '2020-06-30') USING(claim_number)
--   where mon.state = 'TX' and mon.claim_number = 'HTX-0138157-00-01'
  where is_ebsl is false
--   and month_knowledge = '2020-04-01'
--   and month_of_loss = '2020-01-01'
  group by 1,2,3,4,5,6,7,8,9