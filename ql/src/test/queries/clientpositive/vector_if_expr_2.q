set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table if exists foo;
create temporary table foo (x int, y int) stored as orc;
insert into foo values(1,1),(2,NULL),(3,1);

-- Fix HIVE-17682 "Vectorization: IF stmt produces wrong results" (IfExprColumnScalar.txt)

EXPLAIN VECTORIZATION EXPRESSION
select x, IF(x > 0,y,0) from foo order by x;

select x, IF(x > 0,y,0) from foo order by x;

SET hive.vectorized.execution.enabled=false;

select x, IF(x > 0,y,0) from foo order by x;