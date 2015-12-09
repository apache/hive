set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

-- orc merge file tests for dynamic partition case

create table orc_merge5 (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) stored as orc;
create table orc_merge5a (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) partitioned by (st double) stored as orc;

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_merge5;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=50000;
SET hive.optimize.index.filter=true;
set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.compute.splits.in.am=true;
set tez.am.grouping.min-size=1000;
set tez.am.grouping.max-size=50000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=false;
set hive.merge.sparkfiles=false;

explain insert overwrite table orc_merge5a partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5;
set hive.exec.orc.default.row.index.stride=1000;
insert overwrite table orc_merge5a partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5 order by userid;
insert into table orc_merge5a partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5 order by userid;
set hive.exec.orc.default.row.index.stride=2000;
insert into table orc_merge5a partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5 order by userid;
insert into table orc_merge5a partition (st) select userid,string1,subtype,decimal1,ts,subtype from orc_merge5 order by userid;

analyze table orc_merge5a partition(st=80.0) compute statistics noscan;
analyze table orc_merge5a partition(st=0.8) compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a/st=80.0/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a/st=0.8/;
show partitions orc_merge5a;
select * from orc_merge5a where userid<=13;

set hive.merge.orcfile.stripe.level=true;
explain alter table orc_merge5a partition(st=80.0) concatenate;
alter table orc_merge5a partition(st=80.0) concatenate;
alter table orc_merge5a partition(st=0.8) concatenate;

analyze table orc_merge5a partition(st=80.0) compute statistics noscan;
analyze table orc_merge5a partition(st=0.8) compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a/st=80.0/;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5a/st=0.8/;
show partitions orc_merge5a;
select * from orc_merge5a where userid<=13;

