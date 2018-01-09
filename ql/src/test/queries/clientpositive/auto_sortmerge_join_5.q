set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
-- small no part, 4 bucket & big no part, 2 bucket

-- SORT_QUERY_RESULTS

CREATE TABLE bucket_small (key string, value string) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/small/000000_0' INTO TABLE bucket_small;
load data local inpath '../../data/files/auto_sortmerge_join/small/000001_0' INTO TABLE bucket_small;
load data local inpath '../../data/files/auto_sortmerge_join/small/000002_0' INTO TABLE bucket_small;
load data local inpath '../../data/files/auto_sortmerge_join/small/000003_0' INTO TABLE bucket_small;

CREATE TABLE bucket_big (key string, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/auto_sortmerge_join/big/000000_0' INTO TABLE bucket_big;
load data local inpath '../../data/files/auto_sortmerge_join/big/000001_0' INTO TABLE bucket_big;

set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.sortmerge.join.to.mapjoin=false;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.AvgPartitionSizeBasedBigTableSelectorForAutoSMJ;

-- Since size is being used to find the big table, the order of the tables in the join does not matter
explain extended select count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;
select count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;

explain extended select count(*) FROM bucket_big a JOIN bucket_small b ON a.key = b.key;
select count(*) FROM bucket_big a JOIN bucket_small b ON a.key = b.key;

set hive.auto.convert.sortmerge.join.to.mapjoin=true;
set hive.auto.convert.join=true;
explain extended select count(*) FROM bucket_big a JOIN bucket_small b ON a.key = b.key;
select count(*) FROM bucket_big a JOIN bucket_small b ON a.key = b.key;
