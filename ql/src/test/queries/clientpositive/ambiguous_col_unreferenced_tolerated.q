--! qt:dataset:src
-- HIVE-29580: duplicate aliases that are never referenced by name stay accepted under CBO
-- (HIVE-19770/HIVE-20215); the non-CBO planner rejects most of these shapes at definition
-- time, see this file's non-CBO mirror ambiguous_col_noncbo_baseline.q.
select t.d from (select 'a' as c, 'b' as c, 'x' as d) t;
with c1 as (select 'a' as c, 'b' as c, 'x' as d)
select d from c1;
select count(1) from (select 'a' as c, 'b' as c) t;
select count(*) from (select key, key from src) subq;
create table wjt1 (k int, v int);
create table wjt2 (k int, w int);
select t.v from (select a.*, b.* from wjt1 a join wjt2 b on a.k = b.k) t;
select * from (select * from (select 'a' as c, 'b' as c) a) b;
select count(*) from src a where exists (select 'x' as c, 'y' as c from src b where b.key = a.key);
select count(*) from src where exists (select 'a' as c, 'b' as c from src);
select count(*) from (select value from src group by value having exists (select 'x' as c, 'y' as c from src b where b.value = src.value)) t;
