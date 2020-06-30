select distinct coverage_e
from dw_prod_extracts.ext_policy_snapshots
where date_snapshot = '2020-06-29'
order by 1;


-- select distinct insurance_score
-- from dw_prod_extracts.ext_policy_snapshots
-- where 1=1 
-- and state = 'CA'
-- -- and insurance_score is null 
-- and date_snapshot = '2020-06-29'

-- select * from dw_prod_extracts.ext_policy_snapshots
-- where coverage_water_backup = '9999999'