select *, unnest(renewals)
from s3.az_nv_policy_rate_capping_data
where renewals is not null