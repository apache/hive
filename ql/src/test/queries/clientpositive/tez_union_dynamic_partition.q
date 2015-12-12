set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
create table dummy(i int);
insert into table dummy values (1);
select * from dummy;

create table partunion1(id1 int) partitioned by (part1 string);

set hive.exec.dynamic.partition.mode=nonstrict;

explain insert into table partunion1 partition(part1)
select temps.* from (
select 1 as id1, '2014' as part1 from dummy 
union all 
select 2 as id1, '2014' as part1 from dummy ) temps;

insert into table partunion1 partition(part1)
select temps.* from (
select 1 as id1, '2014' as part1 from dummy 
union all 
select 2 as id1, '2014' as part1 from dummy ) temps;

select * from partunion1;
