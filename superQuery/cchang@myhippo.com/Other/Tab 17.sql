select state, sum(written_base + written_total_optionals + written_policy_fee)/ COUNT( *)
from dw_prod_extracts.ext_policy_snapshots
where status = 'active'
and date_snapshot = '2020-07-31'
group by 1