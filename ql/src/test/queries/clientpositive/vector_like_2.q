set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table if exists foo;
create temporary table foo (a string) stored as orc;
insert into foo values("some foo"),("some bar"),(null);

-- Fix HIVE-17804 "Vectorization: Bug erroneously causes match for 1st row in batch (SelectStringColLikeStringScalar)"

EXPLAIN VECTORIZATION DETAIL
select a, a like "%bar" from foo order by a;

select a, a like "%bar" from foo order by a;

SET hive.vectorized.execution.enabled=false;

select a, a like "%bar" from foo order by a;