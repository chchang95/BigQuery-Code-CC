-- select DATETIME(createdat, "America/Los_Angeles") as pst_date, * from `datateam-248616.dbt_actuaries.staging_policies`
-- where DATETIME(createdat, "America/Los_Angeles") >= '2021-01-20T13:00:00'

-- select * from `datateam-248616.dbt_actuaries.staging_leads`

-- select DATETIME(createdat, "America/Los_Angeles") as pst_date, * from `datateam-248616.dbt_actuaries.staging_leads`
-- where DATETIME(createdat, "America/Los_Angeles") >= '2021-01-20T13:00:00'
-- and state = 'tx'
-- and product = 'ho3'

select id, bound, data, transaction, policy_info from postgres_public.policies
where createdat >= '2021-01-22'
and bound is false
-- and carrier <> 'canopius'
-- and state = 'tx'
--           and product not in ('ho5')
--           and json_extract_scalar(transaction,'$.quote.premium.total') is not null
--           and state is not null
--           and initial_quote_date is not null
--           and json_extract_scalar(transaction,'$.effective_date') is not null