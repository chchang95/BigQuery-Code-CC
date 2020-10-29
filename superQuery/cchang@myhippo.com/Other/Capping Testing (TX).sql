with x as (
select target_policy_number,JSON_EXTRACT(renewals,'$[0]') as first_renewal, JSON_EXTRACT(renewals,'$[1]') as second_renewal, existing_rate_cap_amount, substring(target_policy_number,1,2) as state
from s3.az_nv_policy_rate_capping_data
where renewals is not null
and _file <> 'ingestion/tx-policies-10-27-2020.json'
)
select target_policy_number, state, first_renewal,

json_extract(existing_rate_cap_amount, '$.current_term') as current_term_cap,
json_extract(existing_rate_cap_amount, '$.next_term') as next_term_cap,

json_extract(first_renewal, '$.rateCapPercentage') as first_renewal_rate_cap,  

json_extract(first_renewal, '$.oldRaterVersionQuote.premium.base') as first_renewal_old_prem_base,  
json_extract(first_renewal, '$.oldRaterVersionQuote.premium.optionals') as first_renewal_old_prem_optionals,
json_extract(first_renewal, '$.oldRaterVersionQuote.premium.fees') as first_renewal_old_prem_fees,
json_extract(first_renewal, '$.oldRaterVersionQuote.premium.total') as first_renewal_old_prem_total,
json_extract(first_renewal, '$.newRaterVersionQuote.premium.base') as first_renewal_new_prem_base,  
json_extract(first_renewal, '$.newRaterVersionQuote.premium.optionals') as first_renewal_new_prem_optionals,
json_extract(first_renewal, '$.newRaterVersionQuote.premium.fees') as first_renewal_new_prem_fees,
json_extract(first_renewal, '$.newRaterVersionQuote.premium.total') as first_renewal_new_prem_total,

json_extract(second_renewal, '$.rateCapPercentage') as first_renewal_rate_cap,  

json_extract(second_renewal, '$.oldRaterVersionQuote.premium.base') as second_renewal_old_prem_base,  
json_extract(second_renewal, '$.oldRaterVersionQuote.premium.optionals') as second_renewal_old_prem_optionals,
json_extract(second_renewal, '$.oldRaterVersionQuote.premium.fees') as second_renewal_old_prem_fees,
json_extract(second_renewal, '$.oldRaterVersionQuote.premium.total') as second_renewal_old_prem_total,
json_extract(second_renewal, '$.newRaterVersionQuote.premium.base') as second_renewal_new_prem_base,  
json_extract(second_renewal, '$.newRaterVersionQuote.premium.optionals') as second_renewal_new_prem_optionals,
json_extract(second_renewal, '$.newRaterVersionQuote.premium.fees') as second_renewal_new_prem_fees,
json_extract(second_renewal, '$.newRaterVersionQuote.premium.total') as second_renewal_new_prem_total,
from x
where first_renewal is not null
and state = 'TX'
order by 2 desc