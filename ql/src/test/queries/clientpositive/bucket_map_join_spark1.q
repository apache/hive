SET hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
CREATE TABLE srcbucket_mapjoin_part_n19 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n19 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n19 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n19 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n19 partition(ds='2008-04-08');

CREATE TABLE srcbucket_mapjoin_part_2_n16 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n16 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n16 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_2_n16 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_2_n16 partition(ds='2008-04-08');

create table bucketmapjoin_hash_result_1_n7 (key bigint , value1 bigint, value2 bigint);
create table bucketmapjoin_hash_result_2_n7 (key bigint , value1 bigint, value2 bigint);

set hive.auto.convert.join = true;

set hive.optimize.bucketmapjoin = true;

create table bucketmapjoin_tmp_result_n9 (key string , value1 string, value2 string);

explain extended
insert overwrite table bucketmapjoin_tmp_result_n9
select a.key, a.value, b.value
from srcbucket_mapjoin_part_n19 a join srcbucket_mapjoin_part_2_n16 b
on a.key=b.key and b.ds="2008-04-08";

insert overwrite table bucketmapjoin_tmp_result_n9
select a.key, a.value, b.value
from srcbucket_mapjoin_part_n19 a join srcbucket_mapjoin_part_2_n16 b
on a.key=b.key and b.ds="2008-04-08";

select count(1) from bucketmapjoin_tmp_result_n9;
insert overwrite table bucketmapjoin_hash_result_1_n7
select sum(hash(key)), sum(hash(value1)), sum(hash(value2)) from bucketmapjoin_tmp_result_n9;

set hive.optimize.bucketmapjoin = false;

explain extended
insert overwrite table bucketmapjoin_tmp_result_n9
select a.key, a.value, b.value
from srcbucket_mapjoin_part_n19 a join srcbucket_mapjoin_part_2_n16 b
on a.key=b.key and b.ds="2008-04-08";

insert overwrite table bucketmapjoin_tmp_result_n9
select a.key, a.value, b.value
from srcbucket_mapjoin_part_n19 a join srcbucket_mapjoin_part_2_n16 b
on a.key=b.key and b.ds="2008-04-08";

select count(1) from bucketmapjoin_tmp_result_n9;
insert overwrite table bucketmapjoin_hash_result_1_n7
select sum(hash(key)), sum(hash(value1)), sum(hash(value2)) from bucketmapjoin_tmp_result_n9;

