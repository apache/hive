create external table test_case (row_seq smallint, row_desc string) stored as parquet;
insert into test_case values (1, 'a');
insert into test_case values (2, 'aa');
insert into test_case values (6, 'aaaaaa');

with base_t as (select row_seq, row_desc,
  case row_seq
    when 1 then '34'
    when 6 then '35'
    when 2 then '36'
  end as zb from test_case where row_seq in (1,2,6))
select row_seq, row_desc, zb from base_t where zb <> '34';

explain cbo with base_t as (select row_seq, row_desc,
  case row_seq
    when 1 then '34'
    when 6 then '35'
    when 2 then '36'
  end as zb from test_case where row_seq in (1,2,6))
select row_seq, row_desc, zb from base_t where zb <> '34';
