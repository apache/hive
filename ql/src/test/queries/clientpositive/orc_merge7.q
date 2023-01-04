set hive.vectorized.execution.enabled=false;
set hive.explain.user=false;

-- SORT_QUERY_RESULTS

-- orc merge file tests for dynamic partition case

create table orc_merge5_n2 (userid bigint, string1 string, subtype double, decimal1 decimal(38,0), ts timestamp) stored as orc;
create table orc_merge5a_n0 (userid bigint, string1 string, subtype double, decimal1 decimal(38,0), ts timestamp) partitioned by (st double) stored as orc;

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_merge5_n2;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=50000;
SET hive.optimize.index.filter=true;
set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.compute.splits.in.am=true;
set tez.grouping.min-size=1000;
set tez.grouping.max-size=50000;
set hive.exec.dynamic.partition=true;

-- 3 mappers
explain insert overwrite table orc_merge5a_n0 partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5_n2;
insert overwrite table orc_merge5a_n0 partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5_n2;
insert overwrite table orc_merge5a_n0 partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5_n2;

-- 3 files total
analyze table orc_merge5a_n0 partition(st=80.0) compute statistics noscan;
analyze table orc_merge5a_n0 partition(st=0.8) compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n0/st=80.0/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n0/st=0.8/;
show partitions orc_merge5a_n0;
select * from orc_merge5a_n0 where userid<=13;

set hive.merge.orcfile.stripe.level=true;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

-- 3 mappers
explain insert overwrite table orc_merge5a_n0 partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5_n2;
insert overwrite table orc_merge5a_n0 partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5_n2;
insert overwrite table orc_merge5a_n0 partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5_n2;

-- 1 file after merging
analyze table orc_merge5a_n0 partition(st=80.0) compute statistics noscan;
analyze table orc_merge5a_n0 partition(st=0.8) compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n0/st=80.0/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n0/st=0.8/;
show partitions orc_merge5a_n0;
select * from orc_merge5a_n0 where userid<=13;

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

insert overwrite table orc_merge5a_n0 partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5_n2;
insert overwrite table orc_merge5a_n0 partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5_n2;
analyze table orc_merge5a_n0 partition(st=80.0) compute statistics noscan;
analyze table orc_merge5a_n0 partition(st=0.8) compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n0/st=80.0/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n0/st=0.8/;
show partitions orc_merge5a_n0;
select * from orc_merge5a_n0 where userid<=13;

set hive.merge.orcfile.stripe.level=true;
explain alter table orc_merge5a_n0 partition(st=80.0) concatenate;
alter table orc_merge5a_n0 partition(st=80.0) concatenate;
alter table orc_merge5a_n0 partition(st=0.8) concatenate;

-- 1 file after merging
analyze table orc_merge5a_n0 partition(st=80.0) compute statistics noscan;
analyze table orc_merge5a_n0 partition(st=0.8) compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n0/st=80.0/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a_n0/st=0.8/;
show partitions orc_merge5a_n0;
select * from orc_merge5a_n0 where userid<=13;

