--! qt:dataset:impala_dataset

explain
select inline(array(struct('abc')));

explain
select 'abc' from simple_char_tbl union all select 'def';

drop table if exists cdpd_18053;

create table cdpd_18053 (i1 int, i2 int);

explain
select * from (select * from cdpd_18053 t
inner join (select 10 int_col) d where
i1 < int_col) q
where i1 <> int_col and int_col = 10;

drop table cdpd_18053;

