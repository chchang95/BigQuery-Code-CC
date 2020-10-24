with x as (
select target_policy_number,JSON_EXTRACT(renewals,'$[0]') as first_renewal, JSON_EXTRACT(renewals,'$[1]') as second_renewal
from s3.az_nv_policy_rate_capping_data
where renewals is not null)
select target_policy_number,
json_extract(first_renewal, '$.oldRaterVersionQuote.premium.base') as first_renewal_prem_base,  
json_extract(first_renewal, '$.oldRaterVersionQuote.premium.optionals') as first_renewal_prem_optionals,
json_extract(first_renewal, '$.oldRaterVersionQuote.premium.fees') as first_renewal_prem_fees,
json_extract(first_renewal, '$.oldRaterVersionQuote.premium.total') as first_renewal_prem_total,
from x