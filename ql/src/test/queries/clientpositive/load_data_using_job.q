set hive.stats.column.autogather=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.join.emit.interval=2;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

-- Single partition
-- Regular load happens.
CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) STORED AS TEXTFILE;
explain load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
select * from srcbucket_mapjoin;

drop table srcbucket_mapjoin;

-- Triggers a Tez job as partition info is missing from load data.
CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin;
load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin;
select * from srcbucket_mapjoin;
drop table srcbucket_mapjoin;

-- Multi partitions
-- Triggers a Tez job as partition info is missing from load data.
CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string, hr int) STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin;
load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin;
select * from srcbucket_mapjoin;
drop table srcbucket_mapjoin;

-- Multi partitions and directory with files (no sub dirs)
CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string, hr int) STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions/subdir' INTO TABLE srcbucket_mapjoin;
load data local inpath '../../data/files/load_data_job/partitions/subdir' INTO TABLE srcbucket_mapjoin;
select * from srcbucket_mapjoin;
drop table srcbucket_mapjoin;

-- Bucketing
CREATE TABLE srcbucket_mapjoin(key int, value string) clustered by (key) sorted by (key) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/bucketing.txt' INTO TABLE srcbucket_mapjoin;
load data local inpath '../../data/files/load_data_job/bucketing.txt' INTO TABLE srcbucket_mapjoin;
select * from srcbucket_mapjoin;
drop table srcbucket_mapjoin;

-- Single partition and bucketing
CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) clustered by (key) sorted by (key) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin;
load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin;
select * from srcbucket_mapjoin;
drop table srcbucket_mapjoin;

-- Multiple partitions and bucketing
CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string, hr int) clustered by (key) sorted by (key) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin;
load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin;
select * from srcbucket_mapjoin;
drop table srcbucket_mapjoin;

-- Multiple partitions, bucketing, and directory with files (no sub dirs)
CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string, hr int) clustered by (key) sorted by (key) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions/subdir' INTO TABLE srcbucket_mapjoin;
load data local inpath '../../data/files/load_data_job/partitions/subdir' INTO TABLE srcbucket_mapjoin;
select * from srcbucket_mapjoin;
drop table srcbucket_mapjoin;

-- Multiple partitions, bucketing, and directory with files and sub dirs
CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string, hr int) clustered by (key) sorted by (key) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions' INTO TABLE srcbucket_mapjoin;
load data local inpath '../../data/files/load_data_job/partitions' INTO TABLE srcbucket_mapjoin;
select * from srcbucket_mapjoin;
drop table srcbucket_mapjoin;

-- Single partition, multiple buckets
CREATE TABLE srcbucket_mapjoin(key int, value string, ds string) partitioned by (hr int) clustered by (key, value) sorted by (key, value) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin;
load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin;
select * from srcbucket_mapjoin;
drop table srcbucket_mapjoin;

-- Load into ORC table using text files
CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) STORED AS ORC;
explain load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin;
load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin;
select * from srcbucket_mapjoin;
drop table srcbucket_mapjoin;