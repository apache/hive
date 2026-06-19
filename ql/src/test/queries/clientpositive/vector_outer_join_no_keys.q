set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

create temporary table foo(x int) stored as orc;
insert into foo values(1),(2);
create temporary table bar(y int) stored as orc;

explain vectorization detail
select count(*) from bar right outer join foo; -- = 2

select count(*) from bar right outer join foo; -- = 2

explain vectorization detail
select count(*) from bar, foo; -- = 0 

select count(*) from bar, foo; -- = 0 