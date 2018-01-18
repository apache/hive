set hive.merge.mapredfiles=true;
set hive.merge.sparkfiles=true;
set hive.auto.convert.join=false;
set mapreduce.job.reduces=1000;

create table dummy (a string);
insert overwrite directory '/tmp/test' select src.key from src join dummy on src.key = dummy.a;
dfs -ls /tmp/test;

-- verify that this doesn't merge for bucketed tables
create table foo (a bigint, b string) clustered by (a) into 256 buckets;
create table bar (a bigint, b string);
insert overwrite table foo select * from bar;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/foo;
