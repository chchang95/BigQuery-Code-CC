
--retention = number of accepted renewals/number of renewals offered
--offered, accepted, then cancelled, identify flag for these situations and the number of dates from renewal acceptance date.
--flat cancel - hippo or client cancels the current term policy back to the effective date
--midterm cancellation - customers maintain policy for a duration of time < 1 full year.

--1) offer a renewal, dont accept the renewal
--2) offer a renewal, end up cancelling current term policy(flat or mid term) , set original_cancellation_flat_flag not equal to true and remove true's from denom
--3) offer a renewal, they accept the renewal, then decide to cancel after accepting (flat or midterm cancel renewal)---> new_renewal_flag_cancelled
--if renewal_cancellation_flat_flag = false and renewal_policy_status = terminated then this policy did have some exposure > 0 i.e. midterm cancellation for renewal
with top as(SELECT 
    upd.update_action,
    upd.date_customer_update_made,
    upd.date_update_effective,          
    dp1.policy_id,                
    dp1.next_policy_id,               
    date(dp1.timestamp_renewal_offered) as date_renewal_offered,              
    dp1.date_effective as original_date_effective,                
    dp2.date_effective as renewal_date_effective,             
    cast(case when dp2.date_activation_update_made IS NULL THEN 0 ELSE 1 END as numeric) AS renewal_accepted,             
    s.cancellation_reason1 as original_cancellation_reason,           
    s.is_cancellation_flat as original_cancellation_flat_flag,                
    s.date_cancellation as original_date_cancellation,        
    s.policy_number as orig_policy_number,      
    s.renewal_number as orig_term_number,     
    snap2.policy_number as renewal_policy_number,       
    snap2.renewal_number as renewal_term_number,         
    effsnap.coverage_a as coverage_a_at_orig_policy_effective,        
    s.coverage_a as coverage_a_at_snapshot_date,
    dp1.channel,              
    dp1.state,                
    dp1.product,              
    dp1.tenure,               
    dp1.carrier,              
    s.written_base + s.written_total_optionals -s.written_optionals_equipment_breakdown - s.written_optionals_service_line as written_total_ex_ebsl_ex_pol_fee         
    -- s.written_total_optionals,      
    -- s.written_total,            
    s.status as policy_status_at_snapshot,           
    snap2.status as renewal_policy_status_at_snapshot,                
    snap2.cancellation_reason1 as renewal_cancellation,               
    snap2.is_cancellation_flat as renewal_cancellation_flat_flag,             
    snap2.date_cancellation as renewal_date_cancellation,             
    -- case when date_trunc(snap2.date_policy_effective, month) = date_trunc(snap2.date_cancellation, month) then true else false end as cancellation_same_month_as_effective,               
    ren.attribution_source,               
    ren.quote_premium_total,              
    ren.quote_premium_fees,               
    ren.quote_premium_optionals,              
    ren.quote_premium_taxes,              
    ren.quote_sum_perils,             
    ren.quote_optionals_service_line,             
    ren.quote_optionals_equipment_breakdown,              
    ren.quote_optionals_personal_injury,              
    ren.coverage_a,               
    ren.coverage_c,               
    ren.calculated_fields_age_of_roof,                
    ren.calculated_fields_age_of_insured,             
    ren.insurance_score,              
    ren.square_footage,               
    ren.deductible,               
    ren.has_personal_property_replacement_cost,               
    ren.extended_rebuilding_cost,             
    2021-cast(ren.year_built as int64) as age_of_home,                
    ren.zip_code,             
    ren.county                
FROM                
    dw_prod.dim_policies dp1 
JOIN dw_prod_extracts.ext_policy_snapshots s ON s.policy_id = dp1.policy_id  and s.date_snapshot = current_date          
left join dw_prod_extracts.ext_policy_snapshots effsnap on effsnap.policy_id = dp1.policy_id and effsnap.date_snapshot = dp1.date_effective
LEFT JOIN dw_prod_extracts.ext_policy_snapshots snap2 ON snap2.policy_id = dp1.next_policy_id and snap2.date_snapshot = current_date               
LEFT JOIN dw_prod.dim_policies dp2 ON dp1.next_policy_id = dp2.policy_id                
LEFT JOIN dw_prod.dim_renewals ren ON dp1.next_policy_id=ren.policy_id    
left join dw_prod.fct_policy_updates upd on s.latest_policy_update_id = upd.policy_update_id
WHERE               
    dp1.timestamp_renewal_offered is not null             
    --and dp1.state='ca'                
    and dp1.carrier <> 'canopius'     
    order by original_date_effective          
),mid as (
select 
case when renewal_accepted = 0 and original_cancellation_reason is not null then date_diff(date_customer_update_made, date_renewal_offered, day) else null end as days_Delta_original,--how long after seeing the renewal did the client ask us to cancel the original term
  case when renewal_accepted = 0 then 0 
    when renewal_accepted = 1 and renewal_date_cancellation = renewal_date_effective then 1 
    else 0 end as new_renewal_flat_cancelled, 
* from top)
,summary as(
SELECT 
m.state,
m.product,
m.channel,
m.tenure,
date_trunc(cast(m.date_renewal_offered as DATE), MONTH) as renewal_quote_month,
date_trunc(cast(m.renewal_date_effective as DATE), MONTH) as renewal_date_effective,
SUM(m.written_coverage_a) as expiring_coverage_a,
SUM(m.written_total) as expiring_premium,
SUM(m.renewal_accepted) AS renewal_accepted_count,
SUM(m.quote_premium_total) as quote_premium_total,
SUM(m.coverage_a) as quote_coverage_a,
COUNT(*) as renewal_offered_count,
SUM(m.new_renewal_flat_cancelled) as renewal_flat_cancelled_count
FROM mid m
GROUP BY 1,2,3,4,5,6
)
select * from top
-- where days_Delta_original is not null
--where renewal_accepted = 1
--and renewal_policy_status = 'terminated'
-- where policy_id = 2252778 -- 2313121 
-- where original_date_cancellation is not null order by 4 desc
