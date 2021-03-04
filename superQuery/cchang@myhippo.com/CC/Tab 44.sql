
select policy_id, claim_number, assigned_adjuster from dw_prod_extracts.ext_claims_inception_to_date
where date_knowledge = '2021-02-28'
and claim_number in
('CAZ-0808147-00-01',
'CCA-0004580-01-01',
'CCA-0036189-01-01',
'CCA-0196213-00-01',
'CCA-0225613-01-01',
'CCA-0401466-00-01',
'CCA-1148557-00-01',
'CCA-1174462-00-01',
'DCA-1259605-00-01',
'DCA-1441475-01-01',
'HAZ-0807186-00-01',
'HAZ-2011574-00-01',
'HCA-0124029-01-01',
'HCA-0178599-01-01',
'HCA-0370340-00-01',
'HCA-0388267-01-01',
'HCA-0391246-01-01',
'HCA-0536802-00-01',
'HCA-0619087-00-01',
'HCA-0961263-00-02',
'HCA-0969606-00-01',
'HCA-1006402-00-01',
'HCA-1025691-00-01',
'HCA-1112234-00-01',
'HCA-1186867-00-01',
'HCA-1187626-00-01',
'HCA-1235867-00-01',
'HCA-1801340-00-01',
'HCA-2114496-00-01',
'HCA-2549768-00-01',
'HCA-3414437-00-01',
'HNV-2433481-00-01'
)