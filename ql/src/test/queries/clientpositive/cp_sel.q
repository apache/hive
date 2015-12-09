set hive.mapred.mode=nonstrict;
explain
select key,value,'hello' as ds, 'world' as hr from srcpart where hr=11 order by 1 limit 1;
select key,value,'hello' as ds, 'world' as hr from srcpart where hr=11 order by 1 limit 1;
set hive.exec.dynamic.partition.mode=nonstrict;
create table testpartbucket (key string, value string) partitioned by (ds string, hr string) clustered by(key) sorted by(key) into 2 buckets;
explain
insert overwrite table testpartbucket partition(ds,hr) select key,value,'hello' as ds, 'world' as hr from srcpart where hr=11;
insert overwrite table testpartbucket partition(ds,hr) select key,value,'hello' as ds, 'world' as hr from srcpart where hr=11;
select * from testpartbucket limit 3;
drop table testpartbucket;
reset hive.exec.dynamic.partition.mode;
