-- SORT_QUERY_RESULTS

SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.support.concurrency=true;
set hive.acid.direct.insert.enabled=true;

drop table if exists dummy_n2;
drop table if exists partunion1;
drop table if exists partunion2;

create table dummy_n2(i int);
insert into table dummy_n2 values (1);
select * from dummy_n2;

create table partunion1(id1 int) partitioned by (part1 string) stored as orc tblproperties('transactional'='true');
create table partunion2(id1 int) partitioned by (part1 string) stored as orc tblproperties('transactional'='true');

insert into table partunion1 partition(part1)
select temps.* from (
select 1 as id1, '2014' as part1 from dummy_n2 
union all 
select 2 as id1, '2014' as part1 from dummy_n2 ) temps;

select * from partunion1;
show partitions partunion1;

insert into table partunion2 partition(part1)
select temps.* from (
select 1 as id1, '2014' as part1 from dummy_n2 
union all 
select 2 as id1, '2015' as part1 from dummy_n2 ) temps;

select * from partunion2;
show partitions partunion2;

drop table if exists dummy_n2;
drop table if exists partunion1;
drop table if exists partunion2;