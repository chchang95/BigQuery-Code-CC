select *, JSON_EXTRACT(data,'$.promotional_score.report.insurance_score') from postgres_public.policies
where id = 2451231