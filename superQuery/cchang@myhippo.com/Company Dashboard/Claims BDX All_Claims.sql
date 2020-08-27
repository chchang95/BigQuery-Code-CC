
  select tbl_source, max(date_knowledge) from dw_staging_extracts.ext_all_claims_combined
  group by 1