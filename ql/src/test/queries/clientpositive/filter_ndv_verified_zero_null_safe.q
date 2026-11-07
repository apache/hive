-- HIVE-29625: equality filters on a column whose NDV is a verified zero.
-- NDV 0 means the column holds no non-null values, so every row is NULL:
-- col <=> NULL (in either operand order) keeps every row, while equality with a
-- non-null value keeps none.

set hive.fetch.task.conversion=none;

create table filter_ndv_zero_ns (i int, s string) stored as orc;
insert into filter_ndv_zero_ns values (null, 'a'), (null, 'b'), (null, 'c'), (null, 'd');
alter table filter_ndv_zero_ns update statistics for column i set('numDVs'='0','numNulls'='4');

explain select * from filter_ndv_zero_ns where i <=> null;
explain select * from filter_ndv_zero_ns where null <=> i;
explain select * from filter_ndv_zero_ns where i <=> 1;
