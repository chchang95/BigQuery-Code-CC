with top as (
 select *, row_number() over(partition by claim_id order by ts_transaction) as rn
 from dw_prod_extracts.ext_claim_transactions
 where 1 = 1
 and transaction_type = 'reserve' 
 and reserve_type = 'loss'
-- and claim_id = 'baee1a2f-30e0-4086-8bd1-6c26c794bcad'
 ), mid as (
 select claim_id, date_transaction_in_customer_tz as date_initial_reserve_set
 from top where rn = 1
 ), final as (
 select ext.claim_id, ext.claim_number, ext.date_of_loss, ext.date_first_notice_of_loss, ext.peril, ext.assigned_adjuster, ext.date_knowledge as date_first_reserve_set, ext.loss_total_gross_reserve as first_loss_total_gross_reserve,  ext.loss_calculated_total_net_paid as first_loss_calculated_total_net_paid, ext.loss_calculated_net_reserve_corrected as first_loss_calculated_net_reserve_corrected
 , ext.loss_calculated_incurred as first_loss_calculated_incurred
 , cur.loss_total_gross_reserve as eoy_loss_total_gross_reserve,  cur.loss_calculated_total_net_paid as eoy_loss_calculated_total_net_paid, cur.loss_calculated_net_reserve_corrected as eoy_loss_calculated_net_reserve_corrected
 , cur.loss_calculated_incurred as eoy_loss_calculated_incurred
 , cur.assigned_adjuster as eoy_assigned_adj
 from dw_prod_extracts.ext_claims_inception_to_date ext
join mid on ext.claim_id = mid.claim_id and ext.date_knowledge = mid.date_initial_reserve_set
join dw_prod_extracts.ext_claims_inception_to_date cur on ext.claim_id = cur.claim_id and cur.date_knowledge = '2021-03-23'
) 
select * from final