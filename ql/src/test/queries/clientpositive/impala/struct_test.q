--! qt:dataset:impala_dataset

explain
select inline(array(struct('abc')));

explain
select 'abc' from simple_char_tbl union all select 'def';

