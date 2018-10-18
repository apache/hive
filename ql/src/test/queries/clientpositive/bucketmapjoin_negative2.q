set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
CREATE TABLE srcbucket_mapjoin_n5(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n5;
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_n5;

CREATE TABLE srcbucket_mapjoin_part_2_n7 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/bmj2/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n7 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj2/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n7 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj2/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n7 partition(ds='2008-04-09');
load data local inpath '../../data/files/bmj2/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n7 partition(ds='2008-04-09');
set hive.cbo.enable=false;
set hive.optimize.bucketmapjoin = true;
create table bucketmapjoin_tmp_result_n3 (key string , value1 string, value2 string);

explain extended
insert overwrite table bucketmapjoin_tmp_result_n3 
select /*+mapjoin(b)*/ a.key, a.value, b.value 
from srcbucket_mapjoin_n5 a join srcbucket_mapjoin_part_2_n7 b 
on a.key=b.key;
