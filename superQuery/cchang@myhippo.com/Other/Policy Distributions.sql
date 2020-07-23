-- select coverage_e, count(*) from dw_prod_extracts.ext_policy_snapshots
-- where date_snapshot = '2020-07-19'
-- -- and coverage_e >
-- and carrier <> 'Canopius'
-- group by 1

select avg(coverage_a) from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-06-30'
and state = 'CA'
and status = 'active'
-- and product = 'HO3'
-- group by 1

-- select distinct state, property_data_address_zip from dw_prod_extracts.ext_policy_snapshots 
-- where date_snapshot = '2020-07-20'
-- and carrier = 'Topa'
-- and status = 'active'
-- and product = 'HO3'
-- group by 1