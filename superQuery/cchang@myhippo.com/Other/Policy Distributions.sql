select avg(coverage_a) from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-06-30'
-- and carrier = 'Topa'
and status = 'active'
-- and product = 'HO3'
group by 1