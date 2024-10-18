-- Copied from bucketmapjoin_negative2.q
set hive.explain.user=true;
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;

CREATE TABLE srcbucket_mapjoin_n5_tmp(key int, value string) STORED AS TEXTFILE;
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n5_tmp;
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_n5_tmp;
CREATE TABLE srcbucket_mapjoin_n5(key int, value string) PARTITIONED BY SPEC(bucket(2, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_n5 SELECT * FROM srcbucket_mapjoin_n5_tmp;

CREATE TABLE srcbucket_mapjoin_part_2_n7_tmp (key int, value string) partitioned by (ds string) STORED AS TEXTFILE;
load data local inpath '../../data/files/bmj2/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n7_tmp partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj2/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n7_tmp partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj2/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n7_tmp partition(ds='2008-04-09');
load data local inpath '../../data/files/bmj2/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n7_tmp partition(ds='2008-04-09');
CREATE TABLE srcbucket_mapjoin_part_2_n7 (key int, value string, ds string) partitioned by spec (ds, bucket(2, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_part_2_n7 SELECT * FROM srcbucket_mapjoin_part_2_n7_tmp;

set hive.convert.join.bucket.mapjoin.tez=true;
create table bucketmapjoin_tmp_result_n3 (key string , value1 string, value2 string);

explain
insert overwrite table bucketmapjoin_tmp_result_n3
select /*+mapjoin(b)*/ a.key, a.value, b.value
from srcbucket_mapjoin_n5 a join srcbucket_mapjoin_part_2_n7 b
on a.key=b.key;
