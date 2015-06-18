set hive.vectorized.execution.enabled=false;

drop table if exists t1_staging;
create table t1_staging(
a string,
b int,
c int,
d string)
partitioned by (e  string)
clustered by(a)
sorted by(a desc)
into 256 buckets stored as textfile;

load data local inpath '../../data/files/sortdp.txt' overwrite into table t1_staging partition (e='epart');

set hive.optimize.sort.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.sorting=true;
set hive.enforce.bucketing=true;

drop table t1;

create table t1(
a string,
b int,
c int,
d string)
partitioned by (e string)
clustered by(a)
sorted by(a desc) into 10 buckets stored as textfile;

insert overwrite table t1 partition(e) select a,b,c,d,'epart' from t1_staging;

select 'bucket_0';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1/e=epart/000000_0;
select 'bucket_2';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1/e=epart/000002_0;
select 'bucket_4';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1/e=epart/000004_0;
select 'bucket_6';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1/e=epart/000006_0;
select 'bucket_8';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1/e=epart/000008_0;

set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.sorting=true;
set hive.enforce.bucketing=true;

-- disable sorted dynamic partition optimization to make sure the results are correct
drop table t1;

create table t1(
a string,
b int,
c int,
d string)
partitioned by (e string)
clustered by(a)
sorted by(a desc) into 10 buckets stored as textfile;

insert overwrite table t1 partition(e) select a,b,c,d,'epart' from t1_staging;

select 'bucket_0';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1/e=epart/000000_0;
select 'bucket_2';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1/e=epart/000002_0;
select 'bucket_4';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1/e=epart/000004_0;
select 'bucket_6';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1/e=epart/000006_0;
select 'bucket_8';
dfs -cat ${hiveconf:hive.metastore.warehouse.dir}/t1/e=epart/000008_0;
