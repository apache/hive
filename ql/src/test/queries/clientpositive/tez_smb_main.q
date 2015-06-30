set hive.explain.user=false;
set hive.join.emit.interval=2;
explain
select * from src a join src1 b on a.key = b.key;

select * from src a join src1 b on a.key = b.key;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');

set hive.enforce.bucketing=true;
set hive.enforce.sorting = true;
set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part;

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

set hive.convert.join.bucket.mapjoin.tez = true;
set hive.auto.convert.sortmerge.join = true;

explain
select count(*)
from tab a join tab_part b on a.key = b.key;

select count(*)
from tab a join tab_part b on a.key = b.key;

set hive.auto.convert.join.noconditionaltask.size=2000;
set hive.mapjoin.hybridgrace.minwbsize=500;
set hive.mapjoin.hybridgrace.minnumpartitions=4;
explain
select count (*)
from tab a join tab_part b on a.key = b.key;

select count(*)
from tab a join tab_part b on a.key = b.key;

set hive.auto.convert.join.noconditionaltask.size=1000;
set hive.mapjoin.hybridgrace.minwbsize=250;
set hive.mapjoin.hybridgrace.minnumpartitions=4;
explain
select count (*)
from tab a join tab_part b on a.key = b.key;

select count(*)
from tab a join tab_part b on a.key = b.key;

set hive.auto.convert.join.noconditionaltask.size=500;
set hive.mapjoin.hybridgrace.minwbsize=125;
set hive.mapjoin.hybridgrace.minnumpartitions=4;
explain select count(*) from tab a join tab_part b on a.key = b.key join src1 c on a.value = c.value;
select count(*) from tab a join tab_part b on a.key = b.key join src1 c on a.value = c.value;

explain select count(*) from tab a join tab_part b on a.value = b.value;
select count(*) from tab a join tab_part b on a.value = b.value;

explain
select count(*) from (select s1.key as key, s1.value as value from tab s1 join tab s3 on s1.key=s3.key
UNION  ALL
select s2.key as key, s2.value as value from tab s2
) a join tab_part b on (a.key = b.key);

set hive.auto.convert.join.noconditionaltask.size=10000;
explain select count(*) from tab a join tab_part b on a.value = b.value;
select count(*) from tab a join tab_part b on a.value = b.value;

explain select count(*) from tab a join tab_part b on a.key = b.key join src1 c on a.value = c.value;
select count(*) from tab a join tab_part b on a.key = b.key join src1 c on a.value = c.value;

explain
select count(*) from (select s1.key as key, s1.value as value from tab s1 join tab s3 on s1.key=s3.key
UNION  ALL
select s2.key as key, s2.value as value from tab s2
) a join tab_part b on (a.key = b.key);

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

