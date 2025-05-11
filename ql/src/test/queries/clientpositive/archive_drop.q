set hive.mapred.mode=nonstrict;
set hive.archive.enabled = true;

create database test_db;

create table test_db.test_tbl (id int, name string) partitioned by (dt date, hr string);

insert overwrite table test_db.test_tbl partition (dt='2025-04-01', hr='11') select 1, 'tom';
insert overwrite table test_db.test_tbl partition (dt='2025-04-01', hr='12') select 2, 'jerry';
insert overwrite table test_db.test_tbl partition (dt='2025-04-01', hr='13') select 3, 'spike';

show partitions test_db.test_tbl;

alter table test_db.test_tbl archive partition (dt='2025-04-01');
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/test_db.db/test_tbl/dt=2025-04-01/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/test_db.db/test_tbl/dt=2025-04-01/data.har/;

show partitions test_db.test_tbl;

alter table test_db.test_tbl drop partition (dt='2025-04-01',hr='12');
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/test_db.db/test_tbl/dt=2025-04-01/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/test_db.db/test_tbl/dt=2025-04-01/data.har/;

show partitions test_db.test_tbl;

select * from test_db.test_tbl;

drop table test_db.test_tbl;
