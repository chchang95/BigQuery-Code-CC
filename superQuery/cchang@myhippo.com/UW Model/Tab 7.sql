select * from postgres_public.leads where id = '7b1373c1-9341-4c51-8db4-f78d6676d9b1'
and cast(initial_quote_date as date) >= '2020-03-20'
--       and state = 'tx'
          and carrier <> 'canopius'
          and product not in ('ho5')
          and json_extract_scalar(transaction,'$.quote.premium.total') is not null
          and state is not null
          and initial_quote_date is not null
          and json_extract_scalar(transaction,'$.effective_date') is not null
          LIMIT 50000

-- select * from dim_policies