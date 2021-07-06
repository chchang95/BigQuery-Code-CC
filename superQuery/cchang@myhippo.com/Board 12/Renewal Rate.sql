
with top as(SELECT 
    dp1.policy_number,
    dp1.date_effective as initial_policy_effective_date,  
    date_trunc(dp1.date_effective, month) as initial_policy_effective_month,
    dp1.policy_id as initial_policy_id,
    dp1.policy_number as initial_policy_number,
    coalesce(effsnap1.written_base,0) + coalesce(effsnap1.written_total_optionals,0) + coalesce(effsnap1.written_policy_fee,0) as initial_written_premium_w_pol_fees,
    effsnap1.coverage_a as initial_coverage_a,
    effsnap1.renewal_number as initial_term_number,
    
    case when dp1.timestamp_renewal_offered is null then 0 else 1 end as renewal_offered_flag,
    coalesce(offersnap1.quote_premium_base,0) + coalesce(offersnap1.quote_premium_optionals,0) + coalesce(offersnap1.quote_policy_fee,0) as initial_at_offer_premium_w_pol_fees,
    offersnap1.coverage_a as initial_at_offer_coverage_a,
    
    coalesce(renoffersnap2.quote_premium_base,0) + coalesce(renoffersnap2.quote_premium_optionals,0) + coalesce(renoffersnap2.quote_policy_fee,0) as renewal_offer_premium_w_pol_fees,
    renoffersnap2.coverage_a as renewal_offer_coverage_a,    
    
    cast(case when dp2.date_activation_update_made IS NULL THEN 0 ELSE 1 END as numeric) AS renewal_accepted_flag,    
    dp2.date_effective as renewal_policy_effective_date,      
    date_trunc(dp2.date_effective, month) as renewal_policy_effective_month,
    date(dp1.timestamp_renewal_offered) as renewal_offered_date,    
    date_trunc(date(dp1.timestamp_renewal_offered),month) as renewal_offered_month,              
    rensnap2.policy_id as renewal_policy_id,
    rensnap2.policy_number as renewal_policy_number,
    coalesce(rensnap2.quote_premium_base,0) + coalesce(rensnap2.quote_premium_optionals,0) + coalesce(rensnap2.quote_policy_fee,0) as renewal_effective_premium_w_pol_fees,
    rensnap2.coverage_a as renewal_effective_coverage_a,
    rensnap2.renewal_number as renewal_term_number,
    
    currsnap1.cancellation_reason1 as original_cancellation_reason,           
    currsnap1.is_cancellation_flat as original_cancellation_flat_flag,                
    currsnap1.date_cancellation as original_date_cancellation,   
    
    currsnap2.cancellation_reason1 as renewal_cancellation,
    currsnap2.is_cancellation_flat as renewal_cancellation_flat_flag,             
    currsnap2.date_cancellation as renewal_date_cancellation,   
    
    dp1.channel,              
    dp1.state,                
    dp1.product,     
    dp1.zip_code,
    dp1.county

FROM                
    dw_prod.dim_policies dp1 
JOIN dw_prod_extracts.ext_policy_snapshots currsnap1 ON currsnap1.policy_id = dp1.policy_id and currsnap1.date_snapshot = '2021-06-29'          
left join dw_prod_extracts.ext_policy_snapshots effsnap1 on effsnap1.policy_id = dp1.policy_id and effsnap1.date_snapshot = dp1.date_effective
left join dw_prod_extracts.ext_policy_snapshots offersnap1 on offersnap1.policy_id = dp1.policy_id and offersnap1.date_snapshot = date(dp1.timestamp_renewal_offered)
LEFT JOIN dw_prod.dim_policies dp2 ON dp1.next_policy_id = dp2.policy_id                
LEFT JOIN dw_prod_extracts.ext_policy_snapshots renoffersnap2 ON renoffersnap2.policy_id = dp1.next_policy_id and renoffersnap2.date_snapshot = date(dp1.timestamp_renewal_offered)     
LEFT JOIN dw_prod_extracts.ext_policy_snapshots rensnap2 ON rensnap2.policy_id = dp1.next_policy_id and rensnap2.date_snapshot = (case when dp2.date_effective > '2021-06-29' then '2021-06-29' else dp2.date_effective end) 
LEFT JOIN dw_prod_extracts.ext_policy_snapshots currsnap2 ON currsnap2.policy_id = dp1.next_policy_id and currsnap2.date_snapshot = '2021-06-29'        
left join dw_prod.fct_policy_updates upd on currsnap1.latest_policy_update_id = upd.policy_update_id
WHERE 1=1
    -- dp1.timestamp_renewal_offered is not null        
    and effsnap1.written_base > 0
    --and dp1.state='ca'                
    and dp1.carrier <> 'canopius'     
    -- order by original_date_effective   
        and dp1.is_rewritten is false

)
-- select 
-- policy_number,
-- state, product, channel, 
-- initial_policy_effective_month,
-- initial_term_number,
-- renewal_offered_flag,
-- renewal_accepted_flag,
-- renewal_policy_effective_month,
-- renewal_offered_month,
-- renewal_term_number,
-- original_cancellation_reason,
-- original_cancellation_flat_flag,

-- sum(initial_written_premium_w_pol_fees) as initial_written_premium_w_pol_fees,
-- sum(initial_coverage_a) as initial_coverage_a,
-- sum(1) as initial_policy_count,

-- sum(case when renewal_offered_flag = 0 then 0 else initial_at_offer_premium_w_pol_fees end) as initial_at_offer_quote_premium_w_pol_fees,
-- sum(case when renewal_offered_flag = 0 then 0 else initial_at_offer_coverage_a end) as initial_at_offer_coverage_a,
-- sum(renewal_offered_flag) as initial_at_offer_policy_count,

-- sum(renewal_offer_premium_w_pol_fees) as renewal_offer_quote_premium_w_pol_fees,
-- sum(renewal_offer_coverage_a) as renewal_offer_coverage_a,

-- sum(case when renewal_accepted_flag = 0 then 0 else renewal_effective_premium_w_pol_fees end) as renewal_effective_premium_w_pol_fees,
-- sum(case when renewal_accepted_flag = 0 then 0 else renewal_effective_coverage_a end) as renewal_effective_coverage_a,
-- sum(renewal_accepted_flag) as renewal_policy_count

-- from top
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13
-- order by initial_policy_effective_month asc
-- limit 100000

select * from top
where 1=1
-- and state = 'ca'
-- and initial_policy_effective_month = '2020-05-01'
and renewal_effective_premium_w_pol_fees = 0
and renewal_accepted_flag <> 0

