--! qt:dataset:alltypesorc
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.vectorized.execution.enabled=true;
set hive.optimize.sort.dynamic.partition.threshold=1;

create table over1k_n1(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           `dec` decimal(4,2),
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over1k' into table over1k_n1;

create table over1k_orc like over1k_n1;
alter table over1k_orc set fileformat orc;
insert overwrite table over1k_orc select * from over1k_n1;

create table over1k_part_orc(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (ds string, t tinyint) stored as orc;

create table over1k_part_limit_orc like over1k_part_orc;
alter table over1k_part_limit_orc set fileformat orc;

create table over1k_part_buck_orc(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si) into 4 buckets stored as orc;

create table over1k_part_buck_sort_orc(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si) 
       sorted by (f) into 4 buckets stored as orc;

-- map-only jobs converted to map-reduce job by hive.optimize.sort.dynamic.partition optimization
explain insert overwrite table over1k_part_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by si;
explain insert overwrite table over1k_part_limit_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 limit 10;
explain insert overwrite table over1k_part_buck_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;
explain insert overwrite table over1k_part_buck_sort_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

insert overwrite table over1k_part_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by si;
insert overwrite table over1k_part_limit_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 limit 10;
insert overwrite table over1k_part_buck_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;
insert overwrite table over1k_part_buck_sort_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;




-- map-reduce jobs modified by hive.optimize.sort.dynamic.partition optimization
explain insert into table over1k_part_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by si;
explain insert into table over1k_part_limit_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 limit 10;
explain insert into table over1k_part_buck_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;
explain insert into table over1k_part_buck_sort_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

insert into table over1k_part_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by si;
insert into table over1k_part_limit_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 limit 10;
insert into table over1k_part_buck_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;
insert into table over1k_part_buck_sort_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

desc formatted over1k_part_orc partition(ds="foo",t=27);
desc formatted over1k_part_orc partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_limit_orc partition(ds="foo",t=27);
desc formatted over1k_part_limit_orc partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_buck_orc partition(t=27);
desc formatted over1k_part_buck_orc partition(t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_buck_sort_orc partition(t=27);
desc formatted over1k_part_buck_sort_orc partition(t="__HIVE_DEFAULT_PARTITION__");

select count(*) from over1k_part_orc;
select count(*) from over1k_part_limit_orc;
select count(*) from over1k_part_buck_orc;
select count(*) from over1k_part_buck_sort_orc;

-- tests for HIVE-6883
create table over1k_part2_orc(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (ds string, t tinyint);

set hive.optimize.sort.dynamic.partition.threshold=-1;
explain insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by i;
set hive.optimize.sort.dynamic.partition.threshold=1;
explain insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by i;
explain insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from (select * from over1k_orc order by i limit 10) tmp where t is null or t=27;

set hive.optimize.sort.dynamic.partition.threshold=-1;
explain insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from over1k_orc where t is null or t=27 group by si,i,b,f,t;
set hive.optimize.sort.dynamic.partition.threshold=1;
-- tests for HIVE-8162, only partition column 't' should be in last RS operator
explain insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from over1k_orc where t is null or t=27 group by si,i,b,f,t;

set hive.optimize.sort.dynamic.partition.threshold=-1;
insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by i;

desc formatted over1k_part2_orc partition(ds="foo",t=27);
desc formatted over1k_part2_orc partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");

-- SORT_BEFORE_DIFF
select * from over1k_part2_orc;
select count(*) from over1k_part2_orc;

set hive.optimize.sort.dynamic.partition.threshold=1;
insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by i;

desc formatted over1k_part2_orc partition(ds="foo",t=27);
desc formatted over1k_part2_orc partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");

-- SORT_BEFORE_DIFF
select * from over1k_part2_orc;
select count(*) from over1k_part2_orc;

-- hadoop-1 does not honor number of reducers in local mode. There is always only 1 reducer irrespective of the number of buckets.
-- Hence all records go to one bucket and all other buckets will be empty. Similar to HIVE-6867. However, hadoop-2 honors number
-- of reducers and records are spread across all reducers. To avoid this inconsistency we will make number of buckets to 1 for this test.
create table over1k_part_buck_sort2_orc(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si)
       sorted by (f) into 1 buckets;

set hive.optimize.sort.dynamic.partition.threshold=-1;
explain insert overwrite table over1k_part_buck_sort2_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;
set hive.optimize.sort.dynamic.partition.threshold=1;
explain insert overwrite table over1k_part_buck_sort2_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

set hive.optimize.sort.dynamic.partition.threshold=-1;
insert overwrite table over1k_part_buck_sort2_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

desc formatted over1k_part_buck_sort2_orc partition(t=27);
desc formatted over1k_part_buck_sort2_orc partition(t="__HIVE_DEFAULT_PARTITION__");

explain select * from over1k_part_buck_sort2_orc;
select * from over1k_part_buck_sort2_orc;
explain select count(*) from over1k_part_buck_sort2_orc;
select count(*) from over1k_part_buck_sort2_orc;

set hive.optimize.sort.dynamic.partition.threshold=1;
insert overwrite table over1k_part_buck_sort2_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

desc formatted over1k_part_buck_sort2_orc partition(t=27);
desc formatted over1k_part_buck_sort2_orc partition(t="__HIVE_DEFAULT_PARTITION__");

explain select * from over1k_part_buck_sort2_orc;
select * from over1k_part_buck_sort2_orc;
explain select count(*) from over1k_part_buck_sort2_orc;
select count(*) from over1k_part_buck_sort2_orc;

set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.optimize.index.filter=true;
set hive.tez.bucket.pruning=true;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.vectorized.execution.reduce.enabled=true;

create table addcolumns_vectorization_true_disallowincompatible_true_fileformat_orc_tinyint
(i int,si smallint)
partitioned by (s string)
clustered by (si) into 2 buckets
stored as orc tblproperties ('transactional'='true');

set hive.optimize.sort.dynamic.partition.threshold=1;
explain insert into table addcolumns_vectorization_true_disallowincompatible_true_fileformat_orc_tinyint partition (s)
  select cint,csmallint, cstring1 from alltypesorc limit 10;

insert into table addcolumns_vectorization_true_disallowincompatible_true_fileformat_orc_tinyint partition (s)
  select cint,csmallint, cstring1 from alltypesorc limit 10;

select cint, csmallint, cstring1 from alltypesorc limit 10;
select * from addcolumns_vectorization_true_disallowincompatible_true_fileformat_orc_tinyint;
