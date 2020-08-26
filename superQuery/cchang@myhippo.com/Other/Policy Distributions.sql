select state, avg(calculated_fields_tiv) from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-07-31'
-- and state = 'CA'
and status = 'active'
-- and product = 'HO3'
-- group by 1