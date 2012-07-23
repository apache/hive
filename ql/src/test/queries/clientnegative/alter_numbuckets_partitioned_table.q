
create table tst1(key string, value string) partitioned by (ds string) clustered by (key) into 10 buckets;

alter table tst1 clustered by (key) into 8 buckets;

set hive.enforce.bucketing=true;
insert overwrite table tst1 partition (ds='1') select key, value from src;

alter table tst1 clustered by (key) into 12 buckets;
