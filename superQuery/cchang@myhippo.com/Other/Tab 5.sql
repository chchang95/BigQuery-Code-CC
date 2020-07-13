select organization_id, organization_name, root_organization_name, organization_type, organization_type_with_producer_split
from dw_prod.dim_organization_mappings_snapshot 
where snapshot_date = '2020-07-13'
order by organization_id