SET hive.vectorized.execution.enabled=false;
drop table if exists dummy_n7;
drop table if exists partunion1_n0;
 
create table dummy_n7(i int);
insert into table dummy_n7 values (1);
select * from dummy_n7;

create table partunion1_n0(id1 int) partitioned by (part1 string) stored as orc;

set hive.merge.tezfiles=true;

explain insert into table partunion1_n0 partition(part1)
select temps.* from (
select 1 as id1, '2014' as part1 from dummy_n7 
union all 
select 2 as id1, '2014' as part1 from dummy_n7 ) temps;

insert into table partunion1_n0 partition(part1)
select 1 as id1, '2014' as part1 from dummy_n7 
union all 
select 2 as id1, '2014' as part1 from dummy_n7;

select * from partunion1_n0;

drop table dummy_n7;
drop table partunion1_n0;
