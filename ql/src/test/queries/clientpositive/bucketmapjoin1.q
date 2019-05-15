SET hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
CREATE TABLE srcbucket_mapjoin_n1(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

CREATE TABLE srcbucket_mapjoin_part_n1 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

CREATE TABLE srcbucket_mapjoin_part_2_n1 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

set hive.optimize.bucketmapjoin = true;

-- empty partitions (HIVE-3205)
explain extended
select /*+mapjoin(b)*/ a.key, a.value, b.value
from srcbucket_mapjoin_part_n1 a join srcbucket_mapjoin_part_2_n1 b
on a.key=b.key where b.ds="2008-04-08";

select /*+mapjoin(b)*/ a.key, a.value, b.value
from srcbucket_mapjoin_part_n1 a join srcbucket_mapjoin_part_2_n1 b
on a.key=b.key where b.ds="2008-04-08";

explain extended
select /*+mapjoin(a)*/ a.key, a.value, b.value
from srcbucket_mapjoin_part_n1 a join srcbucket_mapjoin_part_2_n1 b
on a.key=b.key where b.ds="2008-04-08";

select /*+mapjoin(a)*/ a.key, a.value, b.value
from srcbucket_mapjoin_part_n1 a join srcbucket_mapjoin_part_2_n1 b
on a.key=b.key where b.ds="2008-04-08";

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n1;
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_n1;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n1 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n1 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n1 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n1 partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj2/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n1 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj2/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n1 partition(ds='2008-04-08');

create table bucketmapjoin_hash_result_1_n0 (key bigint , value1 bigint, value2 bigint);
create table bucketmapjoin_hash_result_2_n0 (key bigint , value1 bigint, value2 bigint);

set hive.optimize.bucketmapjoin = true;
create table bucketmapjoin_tmp_result_n0 (key string , value1 string, value2 string);

explain extended
insert overwrite table bucketmapjoin_tmp_result_n0 
select /*+mapjoin(b)*/ a.key, a.value, b.value 
from srcbucket_mapjoin_n1 a join srcbucket_mapjoin_part_n1 b 
on a.key=b.key where b.ds="2008-04-08";

insert overwrite table bucketmapjoin_tmp_result_n0 
select /*+mapjoin(b)*/ a.key, a.value, b.value 
from srcbucket_mapjoin_n1 a join srcbucket_mapjoin_part_n1 b 
on a.key=b.key where b.ds="2008-04-08";

select count(1) from bucketmapjoin_tmp_result_n0;

insert overwrite table bucketmapjoin_hash_result_1_n0
select sum(hash(key)), sum(hash(value1)), sum(hash(value2)) from bucketmapjoin_tmp_result_n0;

set hive.optimize.bucketmapjoin = false;
insert overwrite table bucketmapjoin_tmp_result_n0 
select /*+mapjoin(b)*/ a.key, a.value, b.value 
from srcbucket_mapjoin_n1 a join srcbucket_mapjoin_part_n1 b 
on a.key=b.key where b.ds="2008-04-08";

select count(1) from bucketmapjoin_tmp_result_n0;
insert overwrite table bucketmapjoin_hash_result_2_n0
select sum(hash(key)), sum(hash(value1)), sum(hash(value2)) from bucketmapjoin_tmp_result_n0;


select a.key-b.key, a.value1-b.value1, a.value2-b.value2
from bucketmapjoin_hash_result_1_n0 a left outer join bucketmapjoin_hash_result_2_n0 b
on a.key = b.key;


set hive.optimize.bucketmapjoin = true;
explain extended
insert overwrite table bucketmapjoin_tmp_result_n0 
select /*+mapjoin(a)*/ a.key, a.value, b.value 
from srcbucket_mapjoin_n1 a join srcbucket_mapjoin_part_n1 b 
on a.key=b.key where b.ds="2008-04-08";

insert overwrite table bucketmapjoin_tmp_result_n0 
select /*+mapjoin(a)*/ a.key, a.value, b.value 
from srcbucket_mapjoin_n1 a join srcbucket_mapjoin_part_n1 b 
on a.key=b.key where b.ds="2008-04-08";

select count(1) from bucketmapjoin_tmp_result_n0;


insert overwrite table bucketmapjoin_hash_result_1_n0
select sum(hash(key)), sum(hash(value1)), sum(hash(value2)) from bucketmapjoin_tmp_result_n0;

set hive.optimize.bucketmapjoin = false;
insert overwrite table bucketmapjoin_tmp_result_n0 
select /*+mapjoin(a)*/ a.key, a.value, b.value 
from srcbucket_mapjoin_n1 a join srcbucket_mapjoin_part_n1 b 
on a.key=b.key where b.ds="2008-04-08";

select count(1) from bucketmapjoin_tmp_result_n0;
insert overwrite table bucketmapjoin_hash_result_2_n0
select sum(hash(key)), sum(hash(value1)), sum(hash(value2)) from bucketmapjoin_tmp_result_n0;

select a.key-b.key, a.value1-b.value1, a.value2-b.value2
from bucketmapjoin_hash_result_1_n0 a left outer join bucketmapjoin_hash_result_2_n0 b
on a.key = b.key;
