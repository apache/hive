set hive.vectorized.execution.enabled=false;
set hive.explain.user=false;

-- SORT_QUERY_RESULTS

create table orc_merge5_n5 (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) stored as orc;
create table orc_merge5b_n0 (userid bigint, string1 string, subtype double, decimal1 decimal, ts timestamp) stored as orc;

load data local inpath '../../data/files/orc_split_elim.orc' into table orc_merge5_n5;

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
set hive.merge.sparkfiles=false;

-- 3 mappers
explain insert overwrite table orc_merge5b_n0 select userid,string1,subtype,decimal1,ts from orc_merge5_n5 where userid<=13;
insert overwrite table orc_merge5b_n0 select userid,string1,subtype,decimal1,ts from orc_merge5_n5 where userid<=13;

-- 3 files total
analyze table orc_merge5b_n0 compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5b_n0/;
select * from orc_merge5b_n0;

set hive.merge.orcfile.stripe.level=true;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.sparkfiles=true;

-- 3 mappers
explain insert overwrite table orc_merge5b_n0 select userid,string1,subtype,decimal1,ts from orc_merge5_n5 where userid<=13;
insert overwrite table orc_merge5b_n0 select userid,string1,subtype,decimal1,ts from orc_merge5_n5 where userid<=13;

-- 1 file after merging
analyze table orc_merge5b_n0 compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5b_n0/;
select * from orc_merge5b_n0;

set hive.merge.orcfile.stripe.level=false;
set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.merge.sparkfiles=false;

insert overwrite table orc_merge5b_n0 select userid,string1,subtype,decimal1,ts from orc_merge5_n5 where userid<=13;
analyze table orc_merge5b_n0 compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5b_n0/;
select * from orc_merge5b_n0;

set hive.merge.orcfile.stripe.level=true;
explain alter table orc_merge5b_n0 concatenate;
alter table orc_merge5b_n0 concatenate;

-- 1 file after merging
analyze table orc_merge5b_n0 compute statistics noscan;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/orc_merge5b_n0/;
select * from orc_merge5b_n0;

