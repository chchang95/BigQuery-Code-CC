-- select coverage_e, count(*) from dw_prod_extracts.ext_policy_snapshots
-- where date_snapshot = '2020-07-19'
-- -- and coverage_e >
-- and carrier <> 'Canopius'
-- group by 1

select state, avg(coverage_a) from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-07-20'
and carrier = 'Topa'
and product = 'HO6'
group by 1