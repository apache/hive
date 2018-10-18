set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
CREATE TABLE bucket_small_n16 (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_small_n16 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_small_n16 partition(ds='2008-04-08');

CREATE TABLE bucket_big_n16 (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n16 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n16 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket_big_n16 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket_big_n16 partition(ds='2008-04-08');

load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n16 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n16 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket_big_n16 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket_big_n16 partition(ds='2008-04-09');

set hive.optimize.bucketmapjoin = true;
select /* + MAPJOIN(a) */ count(*) FROM bucket_small_n16 a JOIN bucket_big_n16 b ON a.key = b.key;

set hive.optimize.bucketmapjoin.sortedmerge = true;
select /* + MAPJOIN(a) */ count(*) FROM bucket_small_n16 a JOIN bucket_big_n16 b ON a.key = b.key;

set hive.input.format = org.apache.hadoop.hive.ql.io.HiveInputFormat;
select /* + MAPJOIN(a) */ count(*) FROM bucket_small_n16 a JOIN bucket_big_n16 b ON a.key = b.key;
