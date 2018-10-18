--! qt:dataset:part
set hive.strict.checks.bucketing=false;

-- small no part, 4 bucket & big no part, 2 bucket
CREATE TABLE bucket_small_n13 (key string, value string) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_small_n13;
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_small_n13;
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket_small_n13;
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket_small_n13;

CREATE TABLE bucket_big_n13 (key string, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n13;
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n13;
set hive.cbo.enable=false;
set hive.optimize.bucketmapjoin = true;
explain extended select /*+ MAPJOIN(a) */ count(*) FROM bucket_small_n13 a JOIN bucket_big_n13 b ON a.key = b.key;
select /*+ MAPJOIN(a) */ count(*) FROM bucket_small_n13 a JOIN bucket_big_n13 b ON a.key = b.key;

set hive.optimize.bucketmapjoin.sortedmerge = true;
explain extended select /*+ MAPJOIN(a) */ count(*) FROM bucket_small_n13 a JOIN bucket_big_n13 b ON a.key = b.key;
select /*+ MAPJOIN(a) */ count(*) FROM bucket_small_n13 a JOIN bucket_big_n13 b ON a.key = b.key;
