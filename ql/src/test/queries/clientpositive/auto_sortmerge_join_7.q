set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
-- small 2 part, 4 bucket & big 2 part, 2 bucket
CREATE TABLE bucket_small_n6 (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/small/000000_0' INTO TABLE bucket_small_n6 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/small/000001_0' INTO TABLE bucket_small_n6 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/small/000002_0' INTO TABLE bucket_small_n6 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/small/000003_0' INTO TABLE bucket_small_n6 partition(ds='2008-04-08');

load data local inpath '../../data/files/auto_sortmerge_join/small/000000_0' INTO TABLE bucket_small_n6 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/small/000001_0' INTO TABLE bucket_small_n6 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/small/000002_0' INTO TABLE bucket_small_n6 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/small/000003_0' INTO TABLE bucket_small_n6 partition(ds='2008-04-09');

CREATE TABLE bucket_big_n6 (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n6 partition(ds='2008-04-08');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n6 partition(ds='2008-04-08');

load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big_n6 partition(ds='2008-04-09');
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big_n6 partition(ds='2008-04-09');

set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.sortmerge.join.to.mapjoin=false;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.AvgPartitionSizeBasedBigTableSelectorForAutoSMJ;
-- disable hash joins
set hive.auto.convert.join.noconditionaltask.size=10;

-- Since size is being used to find the big table, the order of the tables in the join does not matter
explain extended select count(*) FROM bucket_small_n6 a JOIN bucket_big_n6 b ON a.key = b.key;
select count(*) FROM bucket_small_n6 a JOIN bucket_big_n6 b ON a.key = b.key;

explain extended select count(*) FROM bucket_big_n6 a JOIN bucket_small_n6 b ON a.key = b.key;
select count(*) FROM bucket_big_n6 a JOIN bucket_small_n6 b ON a.key = b.key;

set hive.auto.convert.sortmerge.join.to.mapjoin=true;
explain extended select count(*) FROM bucket_big_n6 a JOIN bucket_small_n6 b ON a.key = b.key;
select count(*) FROM bucket_big_n6 a JOIN bucket_small_n6 b ON a.key = b.key;
