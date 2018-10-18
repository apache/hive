set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

create table a_n6(s string) stored as orc;
create table b_n4(s string) stored as orc;
insert into table a_n6 values('aaa_n6');
insert into table b_n4 values('aaa_n6');

-- We expect some vectorization due to NULL (void) projection type.
explain vectorization detail
select NULL from a_n6;

select NULL from a_n6;

explain vectorization expression
select NULL as x from a_n6 union distinct select NULL as x from b_n4;

select NULL as x from a_n6 union distinct select NULL as x from b_n4;