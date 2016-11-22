set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.mapjoin.hybridgrace.hashtable=false;
set hive.join.emit.interval=2;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

-- SORT_QUERY_RESULTS

CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part;

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

set hive.auto.convert.sortmerge.join = true;

set hive.auto.convert.join.noconditionaltask.size=500;
CREATE TABLE empty(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

explain
select count(*) from tab s1 join empty s3 on s1.key=s3.key;

select count(*) from tab s1 join empty s3 on s1.key=s3.key;

explain
select * from tab s1 left outer join empty s3 on s1.key=s3.key;

select * from tab s1 left outer join empty s3 on s1.key=s3.key;

explain
select count(*) from tab s1 left outer join tab s2 on s1.key=s2.key join empty s3 on s1.key = s3.key;

select count(*) from tab s1 left outer join tab s2 on s1.key=s2.key join empty s3 on s1.key = s3.key;

explain
select count(*) from tab s1 left outer join empty s2 on s1.key=s2.key join tab s3 on s1.key = s3.key;

select count(*) from tab s1 left outer join empty s2 on s1.key=s2.key join tab s3 on s1.key = s3.key;

explain
select count(*) from empty s1 join empty s3 on s1.key=s3.key;

select count(*) from empty s1 join empty s3 on s1.key=s3.key;

set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.LeftmostBigTableSelectorForAutoSMJ;

explain
select count(*) from empty s1 join tab s3 on s1.key=s3.key;

select count(*) from empty s1 join tab s3 on s1.key=s3.key;

