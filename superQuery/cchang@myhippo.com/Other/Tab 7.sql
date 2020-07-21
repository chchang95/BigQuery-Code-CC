-- select coverage_e, count(*) from dw_prod_extracts.ext_policy_snapshots
-- where date_snapshot = '2020-07-19'
-- -- and coverage_e >
-- and carrier <> 'Canopius'
-- group by 1

select date_diff(date_policy_effective, date_policy_expires, YEAR), count(*) from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-07-20'
and carrier = 'Topa'
group by 1