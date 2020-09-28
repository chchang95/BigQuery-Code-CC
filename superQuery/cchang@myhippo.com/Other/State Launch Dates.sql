select state, min(date_effective) from dw_prod.dim_policies
where product <> 'ho5'
group by 1
order by 2 desc