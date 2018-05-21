--! qt:dataset:src
drop table table_desc1_n2;
drop table table_desc2_n2;
drop table table_desc3;
drop table table_desc4;



create table table_desc1_n2(key string, value string) clustered by (key)
sorted by (key DESC) into 1 BUCKETS;
create table table_desc2_n2(key string, value string) clustered by (key)
sorted by (key DESC, value DESC) into 1 BUCKETS;
create table table_desc3(key string, value1 string, value2 string) clustered by (key)
sorted by (key DESC, value1 DESC,value2 DESC) into 1 BUCKETS;
create table table_desc4(key string, value2 string) clustered by (key)
sorted by (key DESC, value2 DESC) into 1 BUCKETS;

insert overwrite table table_desc1_n2 select key, value from src sort by key DESC;
insert overwrite table table_desc2_n2 select key, value from src sort by key DESC;
insert overwrite table table_desc3 select key, value, concat(value,"_2") as value2 from src sort by key, value, value2 DESC;
insert overwrite table table_desc4 select key, concat(value,"_2") as value2 from src sort by key, value2 DESC;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.cbo.enable=false;
-- columns are sorted by one key in first table, two keys in second table but in same sort order for key. Hence SMB join should pass

explain
select /*+ mapjoin(b) */ count(*) from table_desc1_n2 a join table_desc2_n2 b
on a.key=b.key where a.key < 10;

select /*+ mapjoin(b) */ count(*) from table_desc1_n2 a join table_desc2_n2 b
on a.key=b.key where a.key < 10;

-- columns are sorted by 3 keys(a, b, c) in first table, two keys(a, c) in second table with same sort order. Hence SMB join should not pass

explain
select /*+ mapjoin(b) */ count(*) from table_desc3 a join table_desc4 b
on a.key=b.key and a.value2=b.value2 where a.key < 10;

select /*+ mapjoin(b) */ count(*) from table_desc3 a join table_desc4 b
on a.key=b.key and a.value2=b.value2 where a.key < 10;
