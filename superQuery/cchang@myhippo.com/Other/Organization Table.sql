select organization_id, organization_name, root_organization_name, organization_type, organization_type_with_producer_split
from dw_prod.dim_organization_mappings 
-- where snapshot_date = '2020-07-13'
order by organization_id 

-- select * from dw_prod.dim_organization_mappings_snapshot
-- where snapshot_date = '2020-07-13'
-- -- and organization_type = 'Producer'
-- and organization_name = 'homepoint financial'