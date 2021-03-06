with x as (
select target_policy_number,JSON_EXTRACT(renewals,'$[0]') as first_renewal, JSON_EXTRACT(renewals,'$[1]') as second_renewal, existing_rate_cap_amount, substring(target_policy_number,2,2) as state
from s3.az_nv_policy_rate_capping_data
where renewals is not null
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

json_extract(first_renewal, '$.raterInputs.coverageA') as first_renewal_cov_a,
json_extract(second_renewal, '$.raterInputs.coverageA') as second_renewal_cov_a,

json_extract(first_renewal, '$.raterInputs.ageOfHome') as first_renewal_ageOfHome,
json_extract(second_renewal, '$.raterInputs.ageOfHome') as second_renewal_ageOfHome,

json_extract(first_renewal, '$.raterInputs.ageOfRoof') as first_renewal_ageOfRoof,
json_extract(second_renewal, '$.raterInputs.ageOfRoof') as second_renewal_ageOfRoof,

json_extract(first_renewal, '$.raterInputs.ageOfInsured') as first_renewal_ageOfInsured,
json_extract(second_renewal, '$.raterInputs.ageOfInsured') as second_renewal_ageOfInsured,

json_extract(first_renewal, '$.raterInputs.renewalNumber') as first_renewal_renewalNumber,
json_extract(second_renewal, '$.raterInputs.renewalNumber') as second_renewal_renewalNumber,

json_extract(first_renewal, '$.raterInputs.lossFreeYears') as first_renewal_lossFreeYears,
json_extract(second_renewal, '$.raterInputs.lossFreeYears') as second_renewal_lossFreeYears,


from x
where first_renewal is not null
order by 2 desc