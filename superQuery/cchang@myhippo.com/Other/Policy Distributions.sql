-- select coverage_e, count(*) from dw_prod_extracts.ext_policy_snapshots
-- where date_snapshot = '2020-07-19'
-- -- and coverage_e >
-- and carrier <> 'Canopius'
-- group by 1

select payment_frequency
, ROUND(avg(cast(calculated_fields_coverages_tiv as numeric)),0), avg(written_base + written_total_optionals - written_optionals_equipment_breakdown - written_optionals_service_line)
,count(policy_number)
from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-09-30'
-- and state = 'CA'
and status = 'active'
-- and product = 'HO3'
group by 1
order by 1

-- select distinct state, property_data_address_zip from dw_prod_extracts.ext_policy_snapshots 
-- where date_snapshot = '2020-07-20'
-- and carrier = 'Topa'
-- and status = 'active'
-- and product = 'HO3'
-- group by 1