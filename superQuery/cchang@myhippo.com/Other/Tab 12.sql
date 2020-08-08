-- SELECT
--     mon.month_knowledge,
--     mon.carrier,
--     mon.state,
--     mon.product,
--     month_of_loss,
--     maturity,
--     case when mon.peril = 'wind' or mon.peril = 'hail' then 'Y'
--       when is_catastrophe is true then 'Y'
--       else 'N' end as CAT
--     ,reinsurance_treaty
--     ,case when reinsurance_treaty = 'Spkr20_Classic' then 'Spkr19_GAP' else reinsurance_treaty end as original_treaty
--     ,sum(case when claim_closed_no_total_payment is true then 0 else 1 end) as claim_count_x_cnp
--     ,sum(total_incurred_inception_to_date) as total_incurred_cumulative
--     ,sum(total_incurred_delta_this_month) as total_incurred_incremental
--     ,sum(case when total_incurred_inception_to_date >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_catastrophe is true) then 100000 else total_incurred_inception_to_date end) as capped_NC_total_incurred_cumulative
--     ,sum(case when total_incurred_inception_to_date >= 100000 and not (mon.peril = 'wind' or mon.peril = 'hail' or is_catastrophe is true) then total_incurred_inception_to_date - 100000 else 0 end) as excess_NC_total_incurred_cumulative
--   FROM
--     dw_prod_extracts.ext_claim_monthly mon
--     left join (select claim_number, reinsurance_treaty from dw_prod_extracts.ext_claims_inception_to_date where date_knowledge = @as_of) USING(claim_number)
--   where is_ebsl is false
-- --   group by 1,2,3,4,5,6,7,8,9
  
-- select tbl_source, max(date_knowledge) from dw_prod_extracts.ext_all_claims_combined
-- where 1=1
-- -- and tbl_source = 'topa_tpa_claims'
-- -- and claim_status = 'TBD'
--  group by 1
-- order by 1

-- select distinct peril_group, peril from dw_prod_extracts.ext_all_claims_combined
-- where 1=1
-- and tbl_source = 'topa_tpa_claims'
-- -- -- and claim_status = 'TBD'
-- --  group by 1
-- order by 1

select 
carrier, tbl_source
,count(distinct claim_number) as total_reported_claim_count
,sum(case when claim_status <> 'closed' then 1 else 0 end) as open_claim_count
,sum(case when claim_status = 'closed' then 1 else 0 end) as closed_claim_count
,sum(loss_paid) as total_paid_loss
,sum(expense_paid) as total_paid_ALAE
,sum(loss_paid + loss_net_reserve - recoveries) as total_incurred_loss
,sum(expense_paid + expense_net_reserve) as total_incurred_expense
,sum(recoveries) as recoveries
,sum(loss_paid + loss_net_reserve - recoveries + expense_paid + expense_net_reserve) as total_incurred
from dw_prod_extracts.ext_all_claims_combined
where 1=1
and carrier <> 'Canopius'
and tbl_source = 'topa_tpa_claims'
and date_knowledge = '2020-04-30'
-- and peril not in ('equipment_breakdown', 'service_line')
group by 1,2

-- Paid loss
-- Incurred loss
-- Total Count
-- Open Count
-- Closed Count