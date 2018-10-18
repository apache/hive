set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.mapjoin.hybridgrace.hashtable=false;
set hive.join.emit.interval=2;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

-- SORT_QUERY_RESULTS

CREATE TABLE srcbucket_mapjoin_n7(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part_n5 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part_n8 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n7 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin_n7 partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n8 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n8 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n8 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n8 partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part_n5 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part_n8;

CREATE TABLE tab_n4(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_n4 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n7;

set hive.auto.convert.sortmerge.join = true;

set hive.auto.convert.join.noconditionaltask.size=500;
CREATE TABLE empty_n0(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

explain
select count(*) from tab_n4 s1 join empty_n0 s3 on s1.key=s3.key;

select count(*) from tab_n4 s1 join empty_n0 s3 on s1.key=s3.key;

explain
select * from tab_n4 s1 left outer join empty_n0 s3 on s1.key=s3.key;

select * from tab_n4 s1 left outer join empty_n0 s3 on s1.key=s3.key;

explain
select count(*) from tab_n4 s1 left outer join tab_n4 s2 on s1.key=s2.key join empty_n0 s3 on s1.key = s3.key;

select count(*) from tab_n4 s1 left outer join tab_n4 s2 on s1.key=s2.key join empty_n0 s3 on s1.key = s3.key;

explain
select count(*) from tab_n4 s1 left outer join empty_n0 s2 on s1.key=s2.key join tab_n4 s3 on s1.key = s3.key;

select count(*) from tab_n4 s1 left outer join empty_n0 s2 on s1.key=s2.key join tab_n4 s3 on s1.key = s3.key;

explain
select count(*) from empty_n0 s1 join empty_n0 s3 on s1.key=s3.key;

select count(*) from empty_n0 s1 join empty_n0 s3 on s1.key=s3.key;

set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.LeftmostBigTableSelectorForAutoSMJ;

explain
select count(*) from empty_n0 s1 join tab_n4 s3 on s1.key=s3.key;

select count(*) from empty_n0 s1 join tab_n4 s3 on s1.key=s3.key;

