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
select d, rank() over (order by d desc) as r from pacd_t tablesample (5 rows) order by 2 desc;
select s, count(*) as n, max(d) as m from pacd_t tablesample (5 rows) group by s order by 3 desc;
select d from pacd_t tablesample (5 rows) union all select d + 10 from pacd_t order by 1 desc;
select * from (select d, s from pacd_t sort by d limit 5) q order by 1 desc;

create view pacd_v as select d, s from (select d, s from pacd_t sort by d limit 5) q order by 1 desc limit 5;
select * from pacd_v;

from (select a.d from pacd_t a join pacd_t b on a.d = b.d) j
insert overwrite table pacd_mi_a select d order by 1 desc limit 2
insert overwrite table pacd_mi_b select d order by 1 desc limit 1;
select d from pacd_mi_a order by d;
select d from pacd_mi_b order by d;

-- SORT BY, CLUSTER BY, DISTRIBUTE BY
select d, s from pacd_t tablesample (5 rows) sort by 1 desc;
select d, s from pacd_t tablesample (5 rows) cluster by 1;
explain select d, s from pacd_t tablesample (5 rows) distribute by 1 sort by 2 desc;
select d, s from pacd_t tablesample (5 rows) distribute by 1 sort by 2 desc;

-- INSERT with a column list: positions and names refer to the select list, not to the target columns
create table pacd_xy (x int, y int);
insert into pacd_xy values (1, 20), (3, 10), (2, 30);
create table pacd_xy_t (x int, y int);
create table pacd_xy_t2 (x int, y int);
create table pacd_xy_p (x int, y int) partitioned by (p string);
create table pacd_xy_def (x int default 7, y int);
insert into pacd_xy_t (y, x) select x, y from pacd_xy tablesample (5 rows) order by 1 desc limit 1;
select x, y from pacd_xy_t;
truncate table pacd_xy_t;
insert into pacd_xy_t (y, x) select x, y from pacd_xy tablesample (5 rows) order by 2 desc limit 1;
select x, y from pacd_xy_t;
truncate table pacd_xy_t;
insert into pacd_xy_t (y, x) select x, y from pacd_xy tablesample (5 rows) order by x desc limit 1;
select x, y from pacd_xy_t;
truncate table pacd_xy_t;
insert into pacd_xy_t (x) select y from pacd_xy tablesample (5 rows) order by 1 limit 1;
select x, y from pacd_xy_t;
insert into pacd_xy_p partition (p = 's') (y, x) select x, y from pacd_xy tablesample (5 rows) order by 1 desc limit 1;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into pacd_xy_p partition (p) (y, x, p) select x, y, 'd' from pacd_xy tablesample (5 rows) order by 2 desc limit 1;
select x, y, p from pacd_xy_p order by p;
insert into pacd_xy_def (y) select x from pacd_xy tablesample (5 rows) order by 1 desc limit 1;
select x, y from pacd_xy_def;
truncate table pacd_xy_t;
from (select a.x, a.y from pacd_xy a join pacd_xy b on a.x = b.x) j
insert into table pacd_xy_t (y, x) select x, y order by 1 desc limit 1
insert into table pacd_xy_t2 (y, x) select x, y order by 2 desc limit 1;
select x, y from pacd_xy_t;
select x, y from pacd_xy_t2;
truncate table pacd_xy_t;
explain insert into pacd_xy_t (y, x) select x, y from pacd_xy tablesample (5 rows) sort by 1 desc;
-- with a LIMIT, an INSERT gets a second reduce sink that sorts and partitions on the same select list columns
explain insert into pacd_xy_t (y, x) select x, y from pacd_xy tablesample (5 rows) sort by 1 desc limit 1;
explain insert into pacd_xy_t (y, x) select x, y from pacd_xy tablesample (5 rows) distribute by 1 limit 1;
explain insert into pacd_xy_t (y, x) select x, y from pacd_xy tablesample (5 rows) sort by x desc limit 1;
explain insert into pacd_xy_t (y, x) select a.x, b.y from pacd_xy tablesample (5 rows) a join pacd_xy b on a.x = b.x sort by b.y desc limit 1;
explain insert into pacd_xy_t (y, x) select x, y from pacd_xy tablesample (5 rows) distribute by 1 sort by 2 desc;
explain insert into pacd_xy_t (y, x) select x, y from pacd_xy tablesample (5 rows) cluster by 2;
explain insert into pacd_xy_p partition (p) (y, x, p) select x, y, 'e' from pacd_xy tablesample (5 rows) distribute by 3 sort by 1 desc;
insert into pacd_xy_t (y, x) select * from pacd_xy tablesample (5 rows) order by 1 desc limit 1;
select x, y from pacd_xy_t;
truncate table pacd_xy_t;
insert into pacd_xy_t (y, x) select a.x, b.y from pacd_xy tablesample (5 rows) a join pacd_xy b on a.x = b.x order by b.y desc limit 1;
select x, y from pacd_xy_t;
truncate table pacd_xy_t;
with c as (select x, y from pacd_xy tablesample (5 rows)) insert into pacd_xy_t (y, x) select x, y from c order by 2 desc limit 1;
select x, y from pacd_xy_t;
truncate table pacd_xy_t;
create view pacd_xy_v as select x, y from pacd_xy;
insert into pacd_xy_t (y, x) select v.x, v.y from pacd_xy_v v join (select 1 as one from pacd_xy tablesample (1 rows)) d order by 1 desc limit 1;
select x, y from pacd_xy_t;
truncate table pacd_xy_t;
truncate table pacd_xy_t2;
from pacd_xy tablesample (5 rows)
insert into table pacd_xy_t (y, x) select x, y order by 1 desc limit 1
insert into table pacd_xy_t2 (y, x) select x, y order by 2 desc limit 1;
select x, y from pacd_xy_t;
select x, y from pacd_xy_t2;
truncate table pacd_xy_t;

-- with the return path enabled, CBO declines lateral views
set hive.cbo.returnpath.hiveop=true;
select s, t.c from pacd_t lateral view explode(array(d, -d)) t as c order by 2 desc;
insert into pacd_xy_t (y, x) select x, t.c from pacd_xy lateral view explode(array(y)) t as c order by 1 desc limit 1;
select x, y from pacd_xy_t;
set hive.cbo.returnpath.hiveop=false;

-- a number is a constant when position aliases are disabled
set hive.orderby.position.alias=false;
select d from pacd_t tablesample (5 rows) order by 1 desc;
set hive.orderby.position.alias=true;

-- with CBO disabled, the ORDER BY position resolves to the literal 2, which is not read as a position again
set hive.cbo.enable=false;
select 2 as c, d from pacd_t order by 1;
