-- create table dbt_actuaries.ext_all_claims_combined_20210131 as (
-- select * from `dw_prod_extracts.ext_all_claims_combined`
-- )

-- drop table dbt_cchin.ext_all_claims_combined_20210131

create table dbt_actuaries.ext_today_knowledge_policy_monthly_premiums_20210131 as (
select * from `dw_prod_extracts.ext_policy_monthly_premiums` 
)

-- drop table dbt_actuaries.ext_today_knowledge_policy_monthly_premiums_20210131

-- create table dbt_actuaries.ext_today_knowledge_policy_monthly_premiums_20210131 as (
-- select * from dbt_cchin.ext_today_knowledge_policy_monthly_premiums_20210131 
-- )

-- create table dbt_actuaries.ext_all_claims_combined_20210131 as (
-- select * from dbt_cchin.ext_all_claims_combined_20210131
-- )