--! qt:dataset:src
set hive.merge.mapredfiles=true;
set hive.auto.convert.join=false;
set mapreduce.job.reduces=1000;

create table dummy_n3 (a string);
insert overwrite directory '/tmp/test' select src.key from src join dummy_n3 on src.key = dummy_n3.a;
dfs -ls /tmp/test;

-- verify that this doesn't merge for bucketed tables
create table foo_n6 (a bigint, b string) clustered by (a) into 256 buckets;
create table bar_n1 (a bigint, b string);
insert overwrite table foo_n6 select * from bar_n1;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/foo_n6;
