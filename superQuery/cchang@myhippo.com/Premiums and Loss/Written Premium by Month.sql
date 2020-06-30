-- select date_knowledge,
-- sum(written_base) as written_base
-- from `dw_prod_extracts.ext_policy_monthly_premiums` 
-- where date_knowledge in ('2020-05-31','2020-04-30')
-- and carrier <> 'Canopius'
-- group by 1


-- select 
-- date_report_period_start as calendar_month
-- , date_report_period_end 
-- , date_knowledge
-- , carrier
-- ,sum(written_base) as written_base
-- from dw_prod_extracts.ext_policy_monthly_premiums
-- where 1 = 1 
-- and date_knowledge = date_report_period_end--'2020-05-31'
-- --and date_report_period_start = '2020-05-01'
-- and carrier <> 'Canopius'
-- group by 1, 2, 3, 4
-- order by 2 desc

select date_knowledge,
sum(written_base) as written_base
from `dw_prod_extracts.ext_policy_monthly_premiums` 
where date_knowledge in ('2020-05-31','2020-04-30')
and carrier <> 'Canopius'
group by 1

--V2