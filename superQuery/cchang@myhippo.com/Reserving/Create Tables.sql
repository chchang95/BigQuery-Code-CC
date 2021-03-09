create table dbt_actuaries.ext_all_claims_combined_20210308 as (
select * from `dw_prod_extracts.ext_all_claims_combined`
)