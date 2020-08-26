-- select coverage_e, count(*) from dw_prod_extracts.ext_policy_snapshots
-- where date_snapshot = '2020-07-19'
-- -- and coverage_e >
-- and carrier <> 'Canopius'
-- group by 1

select state, ROUND(avg(cast(calculated_fields_coverages_tiv as numeric)),0) from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-07-31'
-- and state = 'CA'
and status = 'active'
and product = 'HO3'
group by 1
order by 1

-- select distinct state, property_data_address_zip from dw_prod_extracts.ext_policy_snapshots 
-- where date_snapshot = '2020-07-20'
-- and carrier = 'Topa'
-- and status = 'active'
-- and product = 'HO3'
-- group by 1