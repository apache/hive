set hive.stats.column.autogather=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.join.emit.interval=2;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

-- SORT_QUERY_RESULTS

-- Single partition
-- Regular load happens.
CREATE TABLE srcbucket_mapjoin_n8(key int, value string) partitioned by (ds string) STORED AS TEXTFILE;
explain load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n8 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n8 partition(ds='2008-04-08');
select * from srcbucket_mapjoin_n8;

drop table srcbucket_mapjoin_n8;

-- Triggers a Tez job as partition info is missing from load data.
CREATE TABLE srcbucket_mapjoin_n8(key int, value string) partitioned by (ds string) STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin_n8;
load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin_n8;
select * from srcbucket_mapjoin_n8;
drop table srcbucket_mapjoin_n8;

-- Multi partitions
-- Triggers a Tez job as partition info is missing from load data.
CREATE TABLE srcbucket_mapjoin_n8(key int, value string) partitioned by (ds string, hr int) STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin_n8;
load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin_n8;
select * from srcbucket_mapjoin_n8;
drop table srcbucket_mapjoin_n8;

-- Multi partitions and directory with files (no sub dirs)
CREATE TABLE srcbucket_mapjoin_n8(key int, value string) partitioned by (ds string, hr int) STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions/subdir' INTO TABLE srcbucket_mapjoin_n8;
load data local inpath '../../data/files/load_data_job/partitions/subdir' INTO TABLE srcbucket_mapjoin_n8;
select * from srcbucket_mapjoin_n8;
drop table srcbucket_mapjoin_n8;

-- Bucketing
CREATE TABLE srcbucket_mapjoin_n8(key int, value string) clustered by (key) sorted by (key) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/bucketing.txt' INTO TABLE srcbucket_mapjoin_n8;
load data local inpath '../../data/files/load_data_job/bucketing.txt' INTO TABLE srcbucket_mapjoin_n8;
select * from srcbucket_mapjoin_n8;
drop table srcbucket_mapjoin_n8;

-- Single partition and bucketing
CREATE TABLE srcbucket_mapjoin_n8(key int, value string) partitioned by (ds string) clustered by (key) sorted by (key) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin_n8;
load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin_n8;
select * from srcbucket_mapjoin_n8;
drop table srcbucket_mapjoin_n8;

-- Multiple partitions and bucketing
CREATE TABLE srcbucket_mapjoin_n8(key int, value string) partitioned by (ds string, hr int) clustered by (key) sorted by (key) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin_n8;
load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin_n8;
select * from srcbucket_mapjoin_n8;
drop table srcbucket_mapjoin_n8;

-- Multiple partitions, bucketing, and directory with files (no sub dirs)
CREATE TABLE srcbucket_mapjoin_n8(key int, value string) partitioned by (ds string, hr int) clustered by (key) sorted by (key) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions/subdir' INTO TABLE srcbucket_mapjoin_n8;
load data local inpath '../../data/files/load_data_job/partitions/subdir' INTO TABLE srcbucket_mapjoin_n8;
select * from srcbucket_mapjoin_n8;
drop table srcbucket_mapjoin_n8;

-- Multiple partitions, bucketing, and directory with files and sub dirs
CREATE TABLE srcbucket_mapjoin_n8(key int, value string) partitioned by (ds string, hr int) clustered by (key) sorted by (key) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions' INTO TABLE srcbucket_mapjoin_n8;
load data local inpath '../../data/files/load_data_job/partitions' INTO TABLE srcbucket_mapjoin_n8;
select * from srcbucket_mapjoin_n8;
drop table srcbucket_mapjoin_n8;

-- Single partition, multiple buckets
CREATE TABLE srcbucket_mapjoin_n8(key int, value string, ds string) partitioned by (hr int) clustered by (key, value) sorted by (key, value) into 5 buckets STORED AS TEXTFILE;
explain load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin_n8;
load data local inpath '../../data/files/load_data_job/partitions/load_data_2_partitions.txt' INTO TABLE srcbucket_mapjoin_n8;
select * from srcbucket_mapjoin_n8;
drop table srcbucket_mapjoin_n8;

-- Load into ORC table using text files
CREATE TABLE srcbucket_mapjoin_n8(key int, value string) partitioned by (ds string) STORED AS ORC;
explain load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin_n8
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';
load data local inpath '../../data/files/load_data_job/load_data_1_partition.txt' INTO TABLE srcbucket_mapjoin_n8
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';
select * from srcbucket_mapjoin_n8;
drop table srcbucket_mapjoin_n8;

-- Load into ACID table using ORC files
set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.optimize.index.filter=true;
set hive.tez.bucket.pruning=true;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE orc_test_txn (`id` integer, name string, dept string) PARTITIONED BY (year integer) STORED AS ORC TBLPROPERTIES('transactional'='true');
explain load data local inpath '../../data/files/load_data_job_acid' into table orc_test_txn;
load data local inpath '../../data/files/load_data_job_acid' into table orc_test_txn;

select * from orc_test_txn;

-- Test Load Overwrite.

load data local inpath '../../data/files/load_data_job_acid' OVERWRITE into table orc_test_txn;

select count(*) from orc_test_txn;
select * from orc_test_txn;

load data local inpath '../../data/files/load_data_job_acid' OVERWRITE into table orc_test_txn;

select count(*) from orc_test_txn;
select * from orc_test_txn;
