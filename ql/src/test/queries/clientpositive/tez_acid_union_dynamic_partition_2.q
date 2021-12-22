-- SORT_QUERY_RESULTS

SET hive.vectorized.execution.enabled=false;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.support.concurrency=true;
set hive.acid.direct.insert.enabled=true;

drop table if exists dummy_n7;
drop table if exists partunion1_n0;
drop table if exists partunion1_n1;

create table dummy_n7(i int);
insert into table dummy_n7 values (1);
select * from dummy_n7;

create table partunion1_n0(id1 int) partitioned by (part1 string) stored as orc tblproperties('transactional'='true');
create table partunion1_n1(id1 int) partitioned by (part1 string) stored as orc tblproperties('transactional'='true');

set hive.merge.tezfiles=true;

insert into table partunion1_n0 partition(part1)
select 1 as id1, '2014' as part1 from dummy_n7 
union all 
select 2 as id1, '2014' as part1 from dummy_n7;

select * from partunion1_n0;

show partitions partunion1_n0;

insert into table partunion1_n1 partition(part1)
select 1 as id1, '2014' as part1 from dummy_n7 
union all 
select 2 as id1, '2015' as part1 from dummy_n7;

select * from partunion1_n1;

show partitions partunion1_n1;

drop table dummy_n7;
drop table partunion1_n0;
drop table partunion1_n1;