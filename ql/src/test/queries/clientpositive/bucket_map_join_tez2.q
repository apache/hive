set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
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

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

analyze table srcbucket_mapjoin compute statistics for columns;
analyze table srcbucket_mapjoin_part compute statistics for columns;
analyze table tab compute statistics for columns;
analyze table tab_part compute statistics for columns;

set hive.auto.convert.join.noconditionaltask.size=1500;
set hive.convert.join.bucket.mapjoin.tez = false;
explain select a.key, b.key from tab_part a join tab_part c on a.key = c.key join tab_part b on a.value = b.value;
set hive.convert.join.bucket.mapjoin.tez = true;
explain select a.key, b.key from tab_part a join tab_part c on a.key = c.key join tab_part b on a.value = b.value;

CREATE TABLE tab1(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab1
select key,value from srcbucket_mapjoin;
analyze table tab1 compute statistics for columns;

-- A negative test as src is not bucketed.
set hive.auto.convert.join.noconditionaltask.size=20000;
set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, a.value, b.value
from tab1 a join src b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, b.value
from tab1 a join src b on a.key = b.key;

set hive.auto.convert.join.noconditionaltask.size=500;
set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, b.key from (select key from tab_part where key > 1) a join (select key from tab_part where key > 2) b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, b.key from (select key from tab_part where key > 1) a join (select key from tab_part where key > 2) b on a.key = b.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, b.key from (select key from tab_part where key > 1) a left outer join (select key from tab_part where key > 2) b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, b.key from (select key from tab_part where key > 1) a left outer join (select key from tab_part where key > 2) b on a.key = b.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, b.key from (select key from tab_part where key > 1) a right outer join (select key from tab_part where key > 2) b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, b.key from (select key from tab_part where key > 1) a right outer join (select key from tab_part where key > 2) b on a.key = b.key;

set hive.auto.convert.join.noconditionaltask.size=300;
set hive.convert.join.bucket.mapjoin.tez = false;
explain select a.key, b.key from (select distinct key from tab) a join tab b on b.key = a.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain select a.key, b.key from (select distinct key from tab) a join tab b on b.key = a.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain select a.value, b.value from (select distinct value from tab) a join tab b on b.key = a.value;
set hive.convert.join.bucket.mapjoin.tez = true;
explain select a.value, b.value from (select distinct value from tab) a join tab b on b.key = a.value;



--multi key
CREATE TABLE tab_part1 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key, value) INTO 4 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_part1 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part;
analyze table tab_part1 compute statistics for columns;

set hive.auto.convert.join.noconditionaltask.size=20000;
set hive.convert.join.bucket.mapjoin.tez = false;
explain
select count(*)
from
(select distinct key,value from tab_part) a join tab b on a.key = b.key and a.value = b.value;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select count(*)
from
(select distinct key,value from tab_part) a join tab b on a.key = b.key and a.value = b.value;
