select policy_id, date_trunc(date_policy_effective, MONTH) as policy_inception_month
, case when organization_id is null then 0 else organization_id end as org_id
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = @date_snapshot