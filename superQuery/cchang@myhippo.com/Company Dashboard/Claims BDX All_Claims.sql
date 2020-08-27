  select tbl_source, max(date_report_period_end) from dw_staging_extracts.ext_all_claims_combined
  group by 1