-- SORT_QUERY_RESULTS

SET hive.vectorized.execution.enabled=false;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.support.concurrency=true;
set hive.acid.direct.insert.enabled=true;

drop table if exists dummy_n7;
drop table if exists partunion1_n0;
drop table if exists partunion1_n1;
drop table if exists partunion1_n2;
drop table if exists partunion1_n3;

create table dummy_n7(i int);
insert into table dummy_n7 values (1);
select * from dummy_n7;

create table partunion1_n0(id1 int) partitioned by (part1 string) stored as orc tblproperties('transactional'='true', 'transactional_properties'='insert_only');
create table partunion1_n1(id1 int) partitioned by (part1 string) stored as orc tblproperties('transactional'='true', 'transactional_properties'='insert_only');
create table partunion1_n2(id1 int) partitioned by (part1 string) stored as orc tblproperties('transactional'='true', 'transactional_properties'='insert_only');
create table partunion1_n3(id1 int) partitioned by (part1 string) stored as orc tblproperties('transactional'='true', 'transactional_properties'='insert_only');

set hive.merge.tezfiles=true;

insert overwrite table partunion1_n0 partition(part1)
select 1 as id1, '2014' as part1 from dummy_n7 
union all 
select 2 as id1, '2014' as part1 from dummy_n7;

select * from partunion1_n0;
show partitions partunion1_n0;

insert overwrite table partunion1_n1 partition(part1)
select 1 as id1, '2014' as part1 from dummy_n7 
union all 
select 2 as id1, '2015' as part1 from dummy_n7;

select * from partunion1_n1;
show partitions partunion1_n1;


insert into table partunion1_n2 partition(part1)
select 1 as id1, '2014' as part1 from dummy_n7 
union all 
select 2 as id1, '2014' as part1 from dummy_n7;

select * from partunion1_n2;
show partitions partunion1_n2;

insert overwrite table partunion1_n3 partition(part1)
select 1 as id1, '2014' as part1 from dummy_n7 
union all 
select 2 as id1, '2015' as part1 from dummy_n7;

select * from partunion1_n3;
show partitions partunion1_n3;

drop table dummy_n7;
drop table partunion1_n0;
drop table partunion1_n1;
drop table partunion1_n2;
drop table partunion1_n3;