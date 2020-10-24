with x as (
select target_policy_number,JSON_EXTRACT(renewals,'$[0]') as first_renewal, JSON_EXTRACT(renewals,'$[1]') as second_renewal
from s3.az_nv_policy_rate_capping_data
where renewals is not null)
select target_policy_number, json_extract(first_renewal, '$.oldRaterVersionQuote.premium') as first_renewal_prem 
from x