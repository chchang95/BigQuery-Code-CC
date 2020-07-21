select coverage_e, count(*) from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-07-19'
group by 1