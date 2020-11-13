with policies as (
select policy_id, policy_number, state, product, carrier, date_policy_effective, status
from dw_prod_extracts.ext_policy_snapshots
where state = 'tx'
and product = 'ho3'
and status = 'active'
)
, combined as (
select efg.*, p.* from dw_prod_extracts.ext_factor_grids efg
left join policies p using(policy_id)
where p.policy_id is not null
)
select distinct name from combined