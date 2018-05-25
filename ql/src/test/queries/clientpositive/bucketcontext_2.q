set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
-- small 1 part, 4 bucket & big 2 part, 2 bucket
CREATE TABLE bucket_small_n8 (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_small_n8 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_small_n8 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket_small_n8 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket_small_n8 partition(ds='2008-04-08');

CREATE TABLE bucket_big_n8 (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n8 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n8 partition(ds='2008-04-08');

load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n8 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n8 partition(ds='2008-04-09');
set hive.cbo.enable=false;
set hive.optimize.bucketmapjoin = true;
explain extended select /*+ MAPJOIN(a) */ count(*) FROM bucket_small_n8 a JOIN bucket_big_n8 b ON a.key = b.key;
select /*+ MAPJOIN(a) */ count(*) FROM bucket_small_n8 a JOIN bucket_big_n8 b ON a.key = b.key;

set hive.optimize.bucketmapjoin.sortedmerge = true;
explain extended select /*+ MAPJOIN(a) */ count(*) FROM bucket_small_n8 a JOIN bucket_big_n8 b ON a.key = b.key;
select /*+ MAPJOIN(a) */ count(*) FROM bucket_small_n8 a JOIN bucket_big_n8 b ON a.key = b.key;
