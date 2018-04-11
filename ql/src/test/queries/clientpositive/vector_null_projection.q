set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

create table a(s string) stored as orc;
create table b(s string) stored as orc;
insert into table a values('aaa');
insert into table b values('aaa');

-- We expect some vectorization due to NULL (void) projection type.
explain vectorization detail
select NULL from a;

select NULL from a;

explain vectorization expression
select NULL as x from a union distinct select NULL as x from b;

select NULL as x from a union distinct select NULL as x from b;