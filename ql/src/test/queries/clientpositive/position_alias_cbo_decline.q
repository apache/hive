-- HIVE-30037: positions in ORDER BY, SORT BY, DISTRIBUTE BY and CLUSTER BY must be resolved
-- when CBO is enabled but declines the statement (TABLESAMPLE, nested SORT BY + LIMIT, ...).

create table pacd_t (d int, s string);
insert into pacd_t values (1, 'a'), (3, 'c'), (2, 'b');
create table pacd_n (d int);
insert into pacd_n values (1), (null), (3), (2);
create table pacd_mi_a (d int);
create table pacd_mi_b (d int);

-- ORDER BY
explain select d, s from pacd_t tablesample (5 rows) order by 1 desc;
select d, s from pacd_t tablesample (5 rows) order by 1 desc;
select * from pacd_t tablesample (5 rows) order by 2 desc;
select * exclude (s) from pacd_t tablesample (5 rows) order by 1 desc;
select 2 as c, d from pacd_t tablesample (5 rows) order by 2 desc;
select s, d from pacd_t tablesample (5 rows) order by 2 desc, 1;
select d, s from pacd_t tablesample (5 rows) order by 1 desc, s;
select distinct d from pacd_t tablesample (5 rows) order by 1 desc;
select d from pacd_n tablesample (5 rows) order by 1 desc nulls first;
select d, rank() over (order by d desc) as r from pacd_t tablesample (5 rows) order by 2;
select s, count(*) as n, max(d) as m from pacd_t tablesample (5 rows) group by s order by 3 desc;
select d from pacd_t tablesample (5 rows) union all select d + 10 from pacd_t order by 1 desc;
select * from (select d, s from pacd_t sort by d limit 5) q order by 1 desc;

create view pacd_v as select d, s from (select d, s from pacd_t sort by d limit 5) q order by 1 desc limit 5;
select * from pacd_v;

from (select a.d from pacd_t a join pacd_t b on a.d = b.d) j
insert overwrite table pacd_mi_a select d order by 1 desc limit 2
insert overwrite table pacd_mi_b select d order by 1 limit 2;
select d from pacd_mi_a order by d;
select d from pacd_mi_b order by d;

-- SORT BY, CLUSTER BY, DISTRIBUTE BY
select d, s from pacd_t tablesample (5 rows) sort by 1 desc;
select d, s from pacd_t tablesample (5 rows) cluster by 1;
explain select d, s from pacd_t tablesample (5 rows) distribute by 1 sort by 2 desc;
select d, s from pacd_t tablesample (5 rows) distribute by 1 sort by 2 desc;

-- with the return path enabled, CBO declines lateral views
set hive.cbo.returnpath.hiveop=true;
select s, t.c from pacd_t lateral view explode(array(d, -d)) t as c order by 2 desc;
set hive.cbo.returnpath.hiveop=false;

-- a number is a constant when position aliases are disabled
set hive.orderby.position.alias=false;
select d from pacd_t tablesample (5 rows) order by 1 desc;
set hive.orderby.position.alias=true;

-- with CBO disabled, the ORDER BY position resolves to the literal 2, which is not read as a position again
set hive.cbo.enable=false;
select 2 as c, d from pacd_t order by 1;
