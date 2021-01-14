--! qt:disabled:Disabled in HIVE-19509

set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

CREATE TABLE srcbucket_mapjoin_n14(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part_n9 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part_n15 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n14 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin_n14 partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n15 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n15 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n15 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n15 partition(ds='2008-04-08');

-- SORT_QUERY_RESULTS

set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part_n9 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part_n15;

CREATE TABLE tab_n8(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_n8 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n14;

analyze table srcbucket_mapjoin_n14 compute statistics for columns;
analyze table srcbucket_mapjoin_part_n15 compute statistics for columns;
analyze table tab_n8 compute statistics for columns;
analyze table tab_part_n9 compute statistics for columns;

set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, a.value, b.value
from tab_n8 a join tab_part_n9 b on a.key = b.key order by a.key, a.value, b.value;
select a.key, a.value, b.value
from tab_n8 a join tab_part_n9 b on a.key = b.key order by a.key, a.value, b.value;

set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, b.value
from tab_n8 a join tab_part_n9 b on a.key = b.key order by a.key, a.value, b.value;
select a.key, a.value, b.value
from tab_n8 a join tab_part_n9 b on a.key = b.key order by a.key, a.value, b.value;


set hive.auto.convert.join.noconditionaltask.size=900;
set hive.convert.join.bucket.mapjoin.tez = false;
explain
select count(*)
from 
(select distinct key from tab_part_n9) a join tab_n8 b on a.key = b.key;
select count(*)
from 
(select distinct key from tab_part_n9) a join tab_n8 b on a.key = b.key;

set hive.convert.join.bucket.mapjoin.tez = true;
explain
select count(*)
from
(select distinct key from tab_part_n9) a join tab_n8 b on a.key = b.key;
select count(*)
from
(select distinct key from tab_part_n9) a join tab_n8 b on a.key = b.key;


set hive.convert.join.bucket.mapjoin.tez = false;
explain
select count(*)
from
(select a.key as key, a.value as value from tab_n8 a join tab_part_n9 b on a.key = b.key) c
join
tab_part_n9 d on c.key = d.key;
select count(*)
from
(select a.key as key, a.value as value from tab_n8 a join tab_part_n9 b on a.key = b.key) c
join
tab_part_n9 d on c.key = d.key;

set hive.convert.join.bucket.mapjoin.tez = true;
explain
select count(*)
from
(select a.key as key, a.value as value from tab_n8 a join tab_part_n9 b on a.key = b.key) c
join
tab_part_n9 d on c.key = d.key;
select count(*)
from
(select a.key as key, a.value as value from tab_n8 a join tab_part_n9 b on a.key = b.key) c
join
tab_part_n9 d on c.key = d.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain
select count(*)
from
tab_part_n9 d
join
(select a.key as key, a.value as value from tab_n8 a join tab_part_n9 b on a.key = b.key) c on c.key = d.key;
select count(*)
from
tab_part_n9 d
join
(select a.key as key, a.value as value from tab_n8 a join tab_part_n9 b on a.key = b.key) c on c.key = d.key;

set hive.convert.join.bucket.mapjoin.tez = true;
explain
select count(*)
from
tab_part_n9 d
join
(select a.key as key, a.value as value from tab_n8 a join tab_part_n9 b on a.key = b.key) c on c.key = d.key;
select count(*)
from
tab_part_n9 d
join
(select a.key as key, a.value as value from tab_n8 a join tab_part_n9 b on a.key = b.key) c on c.key = d.key;

-- one side is really bucketed. srcbucket_mapjoin_n14 is not really a bucketed table.
-- In this case the sub-query is chosen as the big table.
set hive.convert.join.bucket.mapjoin.tez = false;
set hive.auto.convert.join.noconditionaltask.size=1000;
explain
select a.k1, a.v1, b.value
from (select sum(substr(srcbucket_mapjoin_n14.value,5)) as v1, key as k1 from srcbucket_mapjoin_n14 GROUP BY srcbucket_mapjoin_n14.key) a
join tab_n8 b on a.k1 = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.k1, a.v1, b.value
from (select sum(substr(srcbucket_mapjoin_n14.value,5)) as v1, key as k1 from srcbucket_mapjoin_n14 GROUP BY srcbucket_mapjoin_n14.key) a
     join tab_n8 b on a.k1 = b.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.k1, a.v1, b.value
from (select sum(substr(tab_n8.value,5)) as v1, key as k1 from tab_part_n9 join tab_n8 on tab_part_n9.key = tab_n8.key GROUP BY tab_n8.key) a
join tab_n8 b on a.k1 = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.k1, a.v1, b.value
from (select sum(substr(tab_n8.value,5)) as v1, key as k1 from tab_part_n9 join tab_n8 on tab_part_n9.key = tab_n8.key GROUP BY tab_n8.key) a
     join tab_n8 b on a.k1 = b.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.k1, a.v1, b.value
from (select sum(substr(x.value,5)) as v1, x.key as k1 from tab_n8 x join tab_n8 y on x.key = y.key GROUP BY x.key) a
join tab_part_n9 b on a.k1 = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.k1, a.v1, b.value
from (select sum(substr(x.value,5)) as v1, x.key as k1 from tab_n8 x join tab_n8 y on x.key = y.key GROUP BY x.key) a
     join tab_part_n9 b on a.k1 = b.key;

-- multi-way join
set hive.convert.join.bucket.mapjoin.tez = false;
set hive.auto.convert.join.noconditionaltask.size=20000;
explain
select a.key, a.value, b.value
from tab_part_n9 a join tab_n8 b on a.key = b.key join tab_n8 c on a.key = c.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, b.value
from tab_part_n9 a join tab_n8 b on a.key = b.key join tab_n8 c on a.key = c.key;

set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, a.value, c.value
from (select x.key, x.value from tab_part_n9 x join tab_n8 y on x.key = y.key) a join tab_n8 c on a.key = c.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, c.value
from (select x.key, x.value from tab_part_n9 x join tab_n8 y on x.key = y.key) a join tab_n8 c on a.key = c.key;

-- in this case sub-query is the small table
set hive.convert.join.bucket.mapjoin.tez = false;
set hive.auto.convert.join.noconditionaltask.size=900;
explain
select a.key, a.value, b.value
from (select key, sum(substr(srcbucket_mapjoin_n14.value,5)) as value from srcbucket_mapjoin_n14 GROUP BY srcbucket_mapjoin_n14.key) a
join tab_part_n9 b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, b.value
from (select key, sum(substr(srcbucket_mapjoin_n14.value,5)) as value from srcbucket_mapjoin_n14 GROUP BY srcbucket_mapjoin_n14.key) a
     join tab_part_n9 b on a.key = b.key;

set hive.convert.join.bucket.mapjoin.tez = false;
set hive.map.aggr=false;
explain
select a.key, a.value, b.value
from (select key, sum(substr(srcbucket_mapjoin_n14.value,5)) as value from srcbucket_mapjoin_n14 GROUP BY srcbucket_mapjoin_n14.key) a
join tab_part_n9 b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, b.value
from (select key, sum(substr(srcbucket_mapjoin_n14.value,5)) as value from srcbucket_mapjoin_n14 GROUP BY srcbucket_mapjoin_n14.key) a
     join tab_part_n9 b on a.key = b.key;

-- join on non-bucketed column results in shuffle join.
set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, a.value, b.value
from tab_n8 a join tab_part_n9 b on a.value = b.value;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, b.value
from tab_n8 a join tab_part_n9 b on a.value = b.value;

CREATE TABLE tab1_n4(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab1_n4
select key,value from srcbucket_mapjoin_n14;

set hive.auto.convert.join.noconditionaltask.size=20000;
set hive.convert.join.bucket.mapjoin.tez = false;
explain
select a.key, a.value, b.value
from tab1_n4 a join tab_part_n9 b on a.key = b.key;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, b.value
from tab1_n4 a join tab_part_n9 b on a.key = b.key;

-- No map joins should be created.
set hive.convert.join.bucket.mapjoin.tez = false;
set hive.auto.convert.join.noconditionaltask.size=15000;
explain select a.key, b.key from tab_part_n9 a join tab_part_n9 c on a.key = c.key join tab_part_n9 b on a.value = b.value;
set hive.convert.join.bucket.mapjoin.tez = true;
explain select a.key, b.key from tab_part_n9 a join tab_part_n9 c on a.key = c.key join tab_part_n9 b on a.value = b.value;

set hive.convert.join.bucket.mapjoin.tez = false;
-- This wont have any effect as the column ds is partition column which is not bucketed.
explain
select a.key, a.value, b.value
from tab_n8 a join tab_part_n9 b on a.key = b.key and a.ds = b.ds;
set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, b.value
from tab_n8 a join tab_part_n9 b on a.key = b.key and a.ds = b.ds;

-- HIVE-17792 : Enable Bucket Map Join when there are extra keys other than bucketed columns
set hive.auto.convert.join.noconditionaltask.size=20000;
set hive.convert.join.bucket.mapjoin.tez = false;
explain select a.key, a.value, b.value
        from tab_n8 a join tab_part_n9 b on a.key = b.key and a.value = b.value;
select a.key, a.value, b.value
from tab_n8 a join tab_part_n9 b on a.key = b.key and a.value = b.value
order by a.key, a.value, b.value;

set hive.convert.join.bucket.mapjoin.tez = true;
explain select a.key, a.value, b.value
        from tab_n8 a join tab_part_n9 b on a.key = b.key and a.value = b.value;
select a.key, a.value, b.value
from tab_n8 a join tab_part_n9 b on a.key = b.key and a.value = b.value
order by a.key, a.value, b.value;


-- With non-bucketed small table
CREATE TABLE tab2_n4(key int, value string) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;
insert overwrite table tab2_n4 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n14;
analyze table tab2_n4 compute statistics for columns;

set hive.convert.join.bucket.mapjoin.tez = false;
explain select a.key, a.value, b.value
        from tab2_n4 a join tab_part_n9 b on a.key = b.key and a.value = b.value;
select a.key, a.value, b.value
from tab2_n4 a join tab_part_n9 b on a.key = b.key and a.value = b.value
order by a.key, a.value, b.value;

set hive.convert.join.bucket.mapjoin.tez = true;
explain select a.key, a.value, b.value
        from tab2_n4 a join tab_part_n9 b on a.key = b.key and a.value = b.value;
select a.key, a.value, b.value
from tab2_n4 a join tab_part_n9 b on a.key = b.key and a.value = b.value
order by a.key, a.value, b.value;
