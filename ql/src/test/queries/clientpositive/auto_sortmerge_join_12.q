--! qt:dataset:part
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
-- small 1 part, 2 bucket & big 2 part, 4 bucket

CREATE TABLE bucket_small_n15 (key string, value string) partitioned by (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE
TBLPROPERTIES('bucketing_version'='1');
load data local inpath '../../data/files/auto_sortmerge_join/small/000000_0' INTO TABLE bucket_small_n15 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/small/000001_0' INTO TABLE bucket_small_n15 partition(ds='2008-04-08');

CREATE TABLE bucket_big_n15 (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE
TBLPROPERTIES('bucketing_version'='1');
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n15 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n15 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket_big_n15 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket_big_n15 partition(ds='2008-04-08');

load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n15 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n15 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket_big_n15 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket_big_n15 partition(ds='2008-04-09');

set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
-- disable hash joins
set hive.auto.convert.join.noconditionaltask.size=10;

CREATE TABLE bucket_medium (key string, value string) partitioned by (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 3 BUCKETS STORED AS TEXTFILE
TBLPROPERTIES('bucketing_version'='1');
load data local inpath '../../data/files/auto_sortmerge_join/small/000000_0' INTO TABLE bucket_medium partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/small/000001_0' INTO TABLE bucket_medium partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/small/000002_0' INTO TABLE bucket_medium partition(ds='2008-04-08');

explain extended select count(*) FROM bucket_small_n15 a JOIN bucket_medium b ON a.key = b.key JOIN bucket_big_n15 c ON c.key = b.key JOIN bucket_medium d ON c.key = b.key;
select count(*) FROM bucket_small_n15 a JOIN bucket_medium b ON a.key = b.key JOIN bucket_big_n15 c ON c.key = b.key JOIN bucket_medium d ON c.key = b.key;
