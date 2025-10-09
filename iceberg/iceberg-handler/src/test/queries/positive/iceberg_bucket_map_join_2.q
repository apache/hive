-- Copied from bucketmapjoin5.q
set hive.explain.user=true;
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;

CREATE TABLE srcbucket_mapjoin_n0_tmp(key int, value string) STORED AS TEXTFILE;
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n0_tmp;
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_n0_tmp;
CREATE TABLE srcbucket_mapjoin_n0(key int, value string) PARTITIONED BY SPEC (bucket(2, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_n0 SELECT * FROM srcbucket_mapjoin_n0_tmp;

CREATE TABLE srcbucket_mapjoin_part_n0_tmp (key int, value string) partitioned by (ds string) STORED AS TEXTFILE;
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n0_tmp partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n0_tmp partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n0_tmp partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n0_tmp partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n0_tmp partition(ds='2008-04-09');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n0_tmp partition(ds='2008-04-09');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n0_tmp partition(ds='2008-04-09');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n0_tmp partition(ds='2008-04-09');
CREATE TABLE srcbucket_mapjoin_part_n0 (key int, value string, ds string) partitioned by spec (ds, bucket(4, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_part_n0 SELECT * FROM srcbucket_mapjoin_part_n0_tmp;

CREATE TABLE srcbucket_mapjoin_part_2_tmp (key int, value string) partitioned by (ds string) STORED AS TEXTFILE;
load data local inpath '../../data/files/bmj2/000000_0' INTO TABLE srcbucket_mapjoin_part_2_tmp partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj2/000001_0' INTO TABLE srcbucket_mapjoin_part_2_tmp partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj2/000000_0' INTO TABLE srcbucket_mapjoin_part_2_tmp partition(ds='2008-04-09');
load data local inpath '../../data/files/bmj2/000001_0' INTO TABLE srcbucket_mapjoin_part_2_tmp partition(ds='2008-04-09');
CREATE TABLE srcbucket_mapjoin_part_2 (key int, value string, ds string) partitioned by spec (ds, bucket(2, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_part_2 SELECT * FROM srcbucket_mapjoin_part_2_tmp;

create table bucketmapjoin_hash_result_1 (key bigint , value1 bigint, value2 bigint);
create table bucketmapjoin_hash_result_2 (key bigint , value1 bigint, value2 bigint);
set hive.convert.join.bucket.mapjoin.tez=true;
create table bucketmapjoin_tmp_result (key string , value1 string, value2 string);

explain
insert overwrite table bucketmapjoin_tmp_result
select /*+mapjoin(a)*/ a.key, a.value, b.value
from srcbucket_mapjoin_n0 a join srcbucket_mapjoin_part_n0 b
on a.key=b.key;

insert overwrite table bucketmapjoin_tmp_result
select /*+mapjoin(a)*/ a.key, a.value, b.value
from srcbucket_mapjoin_n0 a join srcbucket_mapjoin_part_n0 b
on a.key=b.key;

select count(1) from bucketmapjoin_tmp_result;
insert overwrite table bucketmapjoin_hash_result_1
select sum(hash(key)), sum(hash(value1)), sum(hash(value2)) from bucketmapjoin_tmp_result;

set hive.convert.join.bucket.mapjoin.tez=false;
insert overwrite table bucketmapjoin_tmp_result
select /*+mapjoin(a)*/ a.key, a.value, b.value
from srcbucket_mapjoin_n0 a join srcbucket_mapjoin_part_n0 b
on a.key=b.key;

select count(1) from bucketmapjoin_tmp_result;
insert overwrite table bucketmapjoin_hash_result_2
select sum(hash(key)), sum(hash(value1)), sum(hash(value2)) from bucketmapjoin_tmp_result;

select a.key-b.key, a.value1-b.value1, a.value2-b.value2
from bucketmapjoin_hash_result_1 a left outer join bucketmapjoin_hash_result_2 b
on a.key = b.key;


set hive.convert.join.bucket.mapjoin.tez=true;
explain
insert overwrite table bucketmapjoin_tmp_result
select /*+mapjoin(a)*/ a.key, a.value, b.value
from srcbucket_mapjoin_n0 a join srcbucket_mapjoin_part_2 b
on a.key=b.key;

insert overwrite table bucketmapjoin_tmp_result
select /*+mapjoin(a)*/ a.key, a.value, b.value
from srcbucket_mapjoin_n0 a join srcbucket_mapjoin_part_2 b
on a.key=b.key;

select count(1) from bucketmapjoin_tmp_result;
insert overwrite table bucketmapjoin_hash_result_1
select sum(hash(key)), sum(hash(value1)), sum(hash(value2)) from bucketmapjoin_tmp_result;

set hive.convert.join.bucket.mapjoin.tez=false;
insert overwrite table bucketmapjoin_tmp_result
select /*+mapjoin(a)*/ a.key, a.value, b.value
from srcbucket_mapjoin_n0 a join srcbucket_mapjoin_part_2 b
on a.key=b.key;

select count(1) from bucketmapjoin_tmp_result;
insert overwrite table bucketmapjoin_hash_result_2
select sum(hash(key)), sum(hash(value1)), sum(hash(value2)) from bucketmapjoin_tmp_result;

select a.key-b.key, a.value1-b.value1, a.value2-b.value2
from bucketmapjoin_hash_result_1 a left outer join bucketmapjoin_hash_result_2 b
on a.key = b.key;
