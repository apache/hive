--! qt:disabled:Disabled in HIVE-19509

set hive.stats.column.autogather=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.join.emit.interval=2;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part;

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

set hive.convert.join.bucket.mapjoin.tez = true;
set hive.auto.convert.sortmerge.join = true;

set hive.auto.convert.join.noconditionaltask.size=50;

explain
select count(*) from tab s1 join tab s3 on s1.key=s3.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain
select count(*) from
tab vt1
join
(select rt2.id from
(select t2.key as id, t2.value as od from tab_part t2 order by id, od) rt2) vt2
where vt1.key=vt2.id;

select count(*) from
tab vt1
join
(select rt2.id from
(select t2.key as id, t2.value as od from tab_part t2 order by id, od) rt2) vt2
where vt1.key=vt2.id;

explain
select count(*) from
(select rt2.id from
(select t2.key as id, t2.value as od from tab_part t2 order by id, od) rt2) vt2
join
tab vt1
where vt1.key=vt2.id;

select count(*) from
(select rt2.id from
(select t2.key as id, t2.value as od from tab_part t2 order by id, od) rt2) vt2
join
tab vt1
where vt1.key=vt2.id;

set hive.auto.convert.join=false;

explain
select count(*) from
(select rt1.id from
(select t1.key as id, t1.value as od from tab t1 order by id, od) rt1) vt1
join
(select rt2.id from
(select t2.key as id, t2.value as od from tab_part t2 order by id, od) rt2) vt2
where vt1.id=vt2.id;

select count(*) from
(select rt1.id from
(select t1.key as id, t1.value as od from tab t1 order by id, od) rt1) vt1
join
(select rt2.id from
(select t2.key as id, t2.value as od from tab_part t2 order by id, od) rt2) vt2
where vt1.id=vt2.id;

set hive.auto.convert.sortmerge.join.reduce.side=false;

explain
select count(*) from
(select rt1.id from
(select t1.key as id, t1.value as od from tab t1 order by id, od) rt1) vt1
join
(select rt2.id from
(select t2.key as id, t2.value as od from tab_part t2 order by id, od) rt2) vt2
where vt1.id=vt2.id;

select count(*) from
(select rt1.id from
(select t1.key as id, t1.value as od from tab t1 order by id, od) rt1) vt1
join
(select rt2.id from
(select t2.key as id, t2.value as od from tab_part t2 order by id, od) rt2) vt2
where vt1.id=vt2.id;

-- SMB disabled for external tables
set hive.disable.unsafe.external.table.operations=true;
CREATE EXTERNAL TABLE tab_ext(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_ext partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

set hive.convert.join.bucket.mapjoin.tez = true;
set hive.auto.convert.sortmerge.join = true;
set hive.auto.convert.join.noconditionaltask.size=500;
set test.comment=SMB disabled for external tables;
set test.comment;
explain
select count(*) from tab_ext s1 join tab_ext s3 on s1.key=s3.key;
