select *
from unnest(cast((select renewals from s3.az_nv_policy_rate_capping_data) as array))
where renewals is not null