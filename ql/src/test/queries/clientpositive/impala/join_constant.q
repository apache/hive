--! qt:dataset:impala_dataset

explain cbo physical
select 5 + 3;

explain cbo physical
select * from (select t.* from impala_tpch_lineitem t
inner join (select 10 bigint_col) d where
t.l_linenumber < d.bigint_col ) q
where l_linenumber = 10;


explain
select 5 + 3;

explain
select * from (select t.* from impala_tpch_lineitem t
inner join (select 10 bigint_col) d where
t.l_linenumber < d.bigint_col ) q
where l_linenumber = 10;
