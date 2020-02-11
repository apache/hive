set hive.vectorized.execution.enabled=true;
set hive.llap.io.enabled=true;
set hive.map.aggr=false;
set hive.strict.checks.bucketing=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

CREATE TABLE srcbucket_mapjoin_n13(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part_n8 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part_n14 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n13 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin_n13 partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n14 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n14 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n14 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n14 partition(ds='2008-04-08');

set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part_n8 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part_n14;

CREATE TABLE tab_n7(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_n7 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n13;

set hive.convert.join.bucket.mapjoin.tez = true;
explain vectorization detail
select a.key, a.value, b.value
from tab_n7 a join tab_part_n8 b on a.key = b.key
order by a.key, a.value, b.value
limit 10;
select a.key, a.value, b.value
from tab_n7 a join tab_part_n8 b on a.key = b.key
order by a.key, a.value, b.value
limit 10;



