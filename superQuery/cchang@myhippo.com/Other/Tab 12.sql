select distinct peril_group, peril from dw_prod_extracts.ext_all_claims_combined
where 1=1
and tbl_source = 'topa_tpa_claims'
-- -- and claim_status = 'TBD'
--  group by 1
order by 1