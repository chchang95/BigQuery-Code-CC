select property_data_address_county, property_data_fireline_score,
sum(coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0)) as TIV
FROM dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-08-31'
and product <> 'ho5'
and carrier = 'topa'
and state = 'ca'
and status = 'active'
group by 1,2
order by 1,2

-- select policy_number, property_data_address_county, property_data_fireline_score, date_policy_effective, property_data_address_zip,
-- sum(coalesce(coverage_a,0) + coalesce(coverage_b,0) + coalesce(coverage_c,0) + coalesce(coverage_d,0)) as TIV
-- FROM dw_prod_extracts.ext_policy_snapshots
-- where date_snapshot = '2020-08-31'
-- and product <> 'ho5'
-- -- and product = 'ho3'
-- and carrier = 'topa'
-- and state = 'ca'
-- and status = 'active'
-- and cast(property_data_fireline_score as numeric) > 1
-- group by 1,2,3,4,5
-- order by 1,2