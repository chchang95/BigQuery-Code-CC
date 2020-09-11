select state, COUNT(distinct *)
from dw_prod_extracts.ext_policy_snapshots
where status = 'active'
and date_snapshot = '2020-07-31'