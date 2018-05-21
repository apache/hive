--! qt:dataset:part
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
-- small 1 part, 2 bucket & big 2 part, 4 bucket

CREATE TABLE bucket_small_n11 (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (KEY) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/small/000000_0' INTO TABLE bucket_small_n11 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/small/000001_0' INTO TABLE bucket_small_n11 partition(ds='2008-04-08');

CREATE TABLE bucket_big_n11 (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY(KEY) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n11 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n11 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket_big_n11 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket_big_n11 partition(ds='2008-04-08');

load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n11 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n11 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000002_0' INTO TABLE bucket_big_n11 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000003_0' INTO TABLE bucket_big_n11 partition(ds='2008-04-09');

set hive.auto.convert.join=true;
-- disable hash joins
set hive.auto.convert.join.noconditionaltask.size=10;
explain extended select count(*) FROM bucket_small_n11 a JOIN bucket_big_n11 b ON a.key = b.key;
select count(*) FROM bucket_small_n11 a JOIN bucket_big_n11 b ON a.key = b.key;

set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;
-- Since size is being used to find the big table, the order of the tables in the join does not matter
-- The tables are only bucketed and not sorted, the join should not be converted
-- Currenly, a join is only converted to a sort-merge join without a hint, automatic conversion to
-- bucketized mapjoin is not done
explain extended select count(*) FROM bucket_small_n11 a JOIN bucket_big_n11 b ON a.key = b.key;
select count(*) FROM bucket_small_n11 a JOIN bucket_big_n11 b ON a.key = b.key;
set hive.cbo.enable=false;
-- The join is converted to a bucketed mapjoin with a mapjoin hint
explain extended select /*+ mapjoin(a) */ count(*) FROM bucket_small_n11 a JOIN bucket_big_n11 b ON a.key = b.key;
select /*+ mapjoin(a) */ count(*) FROM bucket_small_n11 a JOIN bucket_big_n11 b ON a.key = b.key;

-- HIVE-7023
explain extended select /*+ MAPJOIN(a,b) */ count(*) FROM bucket_small_n11 a JOIN bucket_big_n11 b ON a.key = b.key JOIN bucket_big_n11 c ON a.key = c.key;
select /*+ MAPJOIN(a,b) */ count(*) FROM bucket_small_n11 a JOIN bucket_big_n11 b ON a.key = b.key JOIN bucket_big_n11 c ON a.key = c.key;
