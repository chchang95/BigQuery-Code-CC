select renewal_number, count(*) from dw_prod_extracts.ext_policy_snapshots 
where date_snapshot = '2020-07-20'
and carrier = 'Topa'
group by 1