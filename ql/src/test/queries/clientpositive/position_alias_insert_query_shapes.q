-- HIVE-30037: Exercise query-block isolation and final SELECT projections after CBO declines.
set hive.cbo.returnpath.hiveop=true;
create table pai_src (a int, b int);
insert into pai_src values (2,10),(3,20),(1,30);
create table pai_target (a int, b int);
create table pai_script (a int, b int);
create table pai_udtf (a int);

insert into pai_target (b,a)
select a,b from (select a,b from pai_src tablesample (5 rows)
union all select a,b from pai_src) q order by 1 desc,2 desc limit 1;
select 'union',a,b from pai_target;
truncate table pai_target;

explain insert into pai_target (b,a)
select max(a),b from pai_src tablesample (5 rows) group by b order by 1 desc limit 1;
insert into pai_target (b,a)
select max(a),b from pai_src tablesample (5 rows) group by b order by 1 desc limit 1;
select 'aggregate',a,b from pai_target;
truncate table pai_target;

explain insert into pai_target (b,a)
select sum(a) over (partition by b),b from pai_src tablesample (5 rows)
order by 1 desc limit 1;
insert into pai_target (b,a)
select sum(a) over (partition by b),b from pai_src tablesample (5 rows)
order by 1 desc limit 1;
select 'window',a,b from pai_target;
truncate table pai_target;

insert into pai_target (b,a)
select t.* from pai_src lateral view inline(array(struct(a,b))) t as x,y
order by 1 desc limit 1;
select 'star',a,b from pai_target;
truncate table pai_target;

insert into pai_target (b,a)
select b,a from (select a,b from pai_src tablesample (5 rows)
order by 1 desc limit 2) q order by 1 desc limit 1;
select 'nested',a,b from pai_target;
truncate table pai_target;

with q as (select a,b from pai_src tablesample (5 rows) order by 1 desc limit 2)
insert into pai_target (b,a) select b,a from q order by 1 desc limit 1;
select 'cte',a,b from pai_target;
truncate table pai_target;

from pai_src
insert into pai_target (b,a) select a,b order by 1 desc limit 1
insert into pai_script (b,a) select transform(b,a) using 'cat' as (x int,y int)
order by 1 desc limit 1;
select 'mixed_ordinary',a,b from pai_target;
select 'mixed_script',a,b from pai_script;
truncate table pai_target;

from pai_src tablesample (5 rows)
insert into pai_target (b,a) select a,b order by 1 desc limit 1
insert into pai_udtf (a) select explode(array(b)) order by 1 desc limit 1;
select 'mixed_ordinary_udtf',a,b from pai_target;
select 'mixed_udtf',a from pai_udtf;
