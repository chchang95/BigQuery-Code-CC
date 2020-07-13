select max(snapshot_date) from dw_prod.dim_organization_mappings_snapshot 
-- group by organization_id