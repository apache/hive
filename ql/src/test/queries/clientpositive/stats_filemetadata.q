--! qt:disabled:disabled by 98c5b637df2d in 2017
--! qt:dataset:src
set hive.mapred.mode=nonstrict;

CREATE TABLE many_files(key string, value string)
partitioned by (ds string)
clustered by (key) into 4 buckets
stored as orc;

insert overwrite table many_files partition (ds='1') select * from src;
insert overwrite table many_files partition (ds='2') select * from src;

dfs -ls -R ${hiveconf:hive.metastore.warehouse.dir}/many_files/;

explain analyze table many_files cache metadata;
analyze table many_files cache metadata;

set hive.fetch.task.conversion=none;

select sum(hash(*)) from many_files;
