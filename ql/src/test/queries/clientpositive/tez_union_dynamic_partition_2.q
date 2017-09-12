drop table if exists dummy;
drop table if exists partunion1;
 
create table dummy(i int);
insert into table dummy values (1);
select * from dummy;

create table partunion1(id1 int) partitioned by (part1 string) stored as orc;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.tezfiles=true;

explain insert into table partunion1 partition(part1)
select temps.* from (
select 1 as id1, '2014' as part1 from dummy 
union all 
select 2 as id1, '2014' as part1 from dummy ) temps;

insert into table partunion1 partition(part1)
select 1 as id1, '2014' as part1 from dummy 
union all 
select 2 as id1, '2014' as part1 from dummy;

select * from partunion1;

drop table dummy;
drop table partunion1;
