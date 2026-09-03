--! qt:dataset:src
-- HIVE-29580: baseline of the non-CBO planner's pre-existing duplicate-alias strictness, left
-- untouched by the ticket. Every statement fails; the FAILED messages pair to statements by
-- order (hive.cli.errors.ignore). Section 1: definition-time rejections that CBO tolerates
-- since HIVE-19770/HIVE-20215 (CBO mirror: ambiguous_col_unreferenced_tolerated.q).
-- Section 2: ambiguous references, which CBO also rejects (ambiguous_col_rejected.q).
set hive.cli.errors.ignore=true;
set hive.cbo.enable=false;

-- ==== definition-time rejections: the duplicate is never referenced by name ====

-- another column of the derived table is referenced
select t.d from (select 'a' as c, 'b' as c, 'x' as d) t;

-- same, through a CTE
with c1 as (select 'a' as c, 'b' as c, 'x' as d)
select d from c1;

-- only counted, no column referenced
select count(*) from (select key, key from src) subq;

-- duplicate created by a wildcard join expansion
create table wj3 (k int, v int);
create table wj4 (k int, w int);
select t.v from (select a.*, b.* from wj3 a join wj4 b on a.k = b.k) t;

-- duplicate crosses two nested star boundaries
select * from (select * from (select 'a' as c, 'b' as c) a) b;

-- duplicate defined inside an EXISTS subquery
select key from src a where exists (select 'x' as c, 'y' as c from src b where b.key = a.key);

-- duplicate defined inside an EXISTS in HAVING
select value, count(1) from src group by value having exists (select 'x' as c, 'y' as c from src b where b.value = src.value);

-- ==== reference-time rejections: the duplicate name is referenced ====

-- expression aliased onto an existing column name, then referenced
FROM (SELECT key, concat(value) AS key FROM src) a SELECT a.key;

-- the HIVE-20215 self-join example
create table t1nc (c1 int);
explain select t.c1 from (select t11.c1, t12.c1 from t1nc as t11 inner join t1nc as t12 on t11.c1 = t12.c1) as t;

-- qualified reference through a consuming CTE
with bse as (select 'a' as c, 'b' as c),
     tpm as (select * from bse)
select tpm.c from tpm;

-- reference into a UNION ALL branch
select t.c from (select 'a' as c, 'b' as c union all select 'x', 'y') t;

-- ambiguous reference used as a GROUP BY key
create table t1gnc (a int);
select s.a from (select a, a from t1gnc) s group by s.a;
