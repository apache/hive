set hive.explain.user=true;
set hive.metastore.aggregate.stats.cache.enabled=false;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1(key STRING, value STRING, val2 STRING) STORED AS TEXTFILE;

CREATE TABLE ss(k1 STRING,v1 STRING,k2 STRING,v2 STRING,k3 STRING,v3 STRING) STORED AS TEXTFILE;

CREATE TABLE sr(k1 STRING,v1 STRING,k2 STRING,v2 STRING,k3 STRING,v3 STRING) STORED AS TEXTFILE;

CREATE TABLE cs(k1 STRING,v1 STRING,k2 STRING,v2 STRING,k3 STRING,v3 STRING) STORED AS TEXTFILE;

INSERT OVERWRITE TABLE ss
SELECT x.key,x.value,y.key,y.value,z.key,z.value
FROM src1 x 
JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08' and z.hr=11);

INSERT OVERWRITE TABLE sr
SELECT x.key,x.value,y.key,y.value,z.key,z.value
FROM src1 x 
JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08' and z.hr=12);

INSERT OVERWRITE TABLE cs
SELECT x.key,x.value,y.key,y.value,z.key,z.value
FROM src1 x 
JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08');


ANALYZE TABLE ss COMPUTE STATISTICS;
ANALYZE TABLE ss COMPUTE STATISTICS FOR COLUMNS k1,v1,k2,v2,k3,v3;

ANALYZE TABLE sr COMPUTE STATISTICS;
ANALYZE TABLE sr COMPUTE STATISTICS FOR COLUMNS k1,v1,k2,v2,k3,v3;

ANALYZE TABLE cs COMPUTE STATISTICS;
ANALYZE TABLE cs COMPUTE STATISTICS FOR COLUMNS k1,v1,k2,v2,k3,v3;

set hive.auto.convert.join=false;

EXPLAIN
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08' and z.hr=11);

EXPLAIN
select 
ss.k1,sr.k2,cs.k3,count(ss.v1),count(sr.v2),count(cs.v3)
FROM 
ss,sr,cs,src d1,src d2,src d3,src1,srcpart
where
    ss.k1 = d1.key 
and sr.k1 = d2.key 
and cs.k1 = d3.key 
and ss.k2 = sr.k2
and ss.k3 = sr.k3
and ss.v1 = src1.value
and ss.v2 = srcpart.value
and sr.v2 = cs.v2
and sr.v3 = cs.v3
and ss.v3='ssv3'
and sr.v1='srv1'
and src1.key = 'src1key'
and srcpart.key = 'srcpartkey'
and d1.value = 'd1value'
and d2.value in ('2000Q1','2000Q2','2000Q3')
and d3.value in ('2000Q1','2000Q2','2000Q3')
group by 
ss.k1,sr.k2,cs.k3
order by 
ss.k1,sr.k2,cs.k3
limit 100;

explain
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union select * from src)z ON (x.value = z.value)
union
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union select * from src)z ON (x.value = z.value);

explain
SELECT x.key, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union select * from src)z ON (x.value = z.value)
union
SELECT x.key, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select key, value from src1 union select key, value from src union select key, value from src)z ON (x.value = z.value)
union
SELECT x.key, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select key, value from src1 union select key, value from src union select key, value from src union select key, value from src)z ON (x.value = z.value);


set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

EXPLAIN
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08' and z.hr=11);

EXPLAIN
select 
ss.k1,sr.k2,cs.k3,count(ss.v1),count(sr.v2),count(cs.v3)
FROM 
ss,sr,cs,src d1,src d2,src d3,src1,srcpart
where
    ss.k1 = d1.key 
and sr.k1 = d2.key 
and cs.k1 = d3.key 
and ss.k2 = sr.k2
and ss.k3 = sr.k3
and ss.v1 = src1.value
and ss.v2 = srcpart.value
and sr.v2 = cs.v2
and sr.v3 = cs.v3
and ss.v3='ssv3'
and sr.v1='srv1'
and src1.key = 'src1key'
and srcpart.key = 'srcpartkey'
and d1.value = 'd1value'
and d2.value in ('2000Q1','2000Q2','2000Q3')
and d3.value in ('2000Q1','2000Q2','2000Q3')
group by 
ss.k1,sr.k2,cs.k3
order by 
ss.k1,sr.k2,cs.k3
limit 100;

explain
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union select * from src)z ON (x.value = z.value)
union
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union select * from src)z ON (x.value = z.value);

explain
SELECT x.key, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union select * from src)z ON (x.value = z.value)
union
SELECT x.key, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select key, value from src1 union select key, value from src union select key, value from src)z ON (x.value = z.value)
union
SELECT x.key, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select key, value from src1 union select key, value from src union select key, value from src union select key, value from src)z ON (x.value = z.value);


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

CREATE TABLE tab2(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab2 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

set hive.convert.join.bucket.mapjoin.tez = false;
set hive.auto.convert.sortmerge.join = true;

set hive.auto.convert.join.noconditionaltask.size=500;

explain 
select s1.key as key, s1.value as value from tab s1 join tab s3 on s1.key=s3.key;

explain 
select s1.key as key, s1.value as value from tab s1 join tab s3 on s1.key=s3.key join tab s2 on s1.value=s2.value;

explain 
select s1.key as key, s1.value as value from tab s1 join tab2 s3 on s1.key=s3.key;

explain 
select s1.key as key, s1.value as value from tab s1 join tab2 s3 on s1.key=s3.key join tab2 s2 on s1.value=s2.value;

explain
select count(*) from (select s1.key as key, s1.value as value from tab s1 join tab s3 on s1.key=s3.key
UNION  ALL
select s2.key as key, s2.value as value from tab s2
) a join tab_part b on (a.key = b.key);

explain
select count(*) from (select s1.key as key, s1.value as value from tab s1 join tab s3 on s1.key=s3.key join tab s2 on s1.value=s2.value
UNION  ALL
select s2.key as key, s2.value as value from tab s2
) a join tab_part b on (a.key = b.key);set hive.explain.user=true;

explain
SELECT x.key, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union all select * from src)z ON (x.value = z.value)
union all
SELECT x.key, y.value
FROM src x JOIN src y ON (x.key = y.key) 
JOIN (select key, value from src1 union all select key, value from src union all select key, value from src)z ON (x.value = z.value)
union all
SELECT x.key, y.value
FROM src1 x JOIN src1 y ON (x.key = y.key) 
JOIN (select key, value from src1 union all select key, value from src union all select key, value from src union all select key, value from src)z ON (x.value = z.value);

explain
SELECT x.key, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union select * from src)z ON (x.value = z.value)
union
SELECT x.key, y.value
FROM src x JOIN src y ON (x.key = y.key) 
JOIN (select key, value from src1 union select key, value from src union select key, value from src)z ON (x.value = z.value)
union
SELECT x.key, y.value
FROM src1 x JOIN src1 y ON (x.key = y.key) 
JOIN (select key, value from src1 union select key, value from src union select key, value from src union select key, value from src)z ON (x.value = z.value);

CREATE TABLE a(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE b(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE c(key STRING, value STRING) STORED AS TEXTFILE;

explain
from
(
SELECT x.key, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union all select * from src)z ON (x.value = z.value)
union all
SELECT x.key, y.value
FROM src x JOIN src y ON (x.key = y.key) 
JOIN (select key, value from src1 union all select key, value from src union all select key, value from src)z ON (x.value = z.value)
union all
SELECT x.key, y.value
FROM src1 x JOIN src1 y ON (x.key = y.key) 
JOIN (select key, value from src1 union all select key, value from src union all select key, value from src union all select key, value from src)z ON (x.value = z.value)
) tmp
INSERT OVERWRITE TABLE a SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE b SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE c SELECT tmp.key, tmp.value;

explain
FROM
( 
SELECT x.key as key, y.value as value from src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union select * from src)z ON (x.value = z.value) 
union
SELECT x.key as key, y.value as value from src x JOIN src y ON (x.key = y.key) 
JOIN (select key, value from src1 union select key, value from src union select key, value from src)z ON (x.value = z.value)
union
SELECT x.key as key, y.value as value from src1 x JOIN src1 y ON (x.key = y.key) 
JOIN (select key, value from src1 union select key, value from src union select key, value from src union select key, value from src)z ON (x.value = z.value)
) tmp
INSERT OVERWRITE TABLE a SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE b SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE c SELECT tmp.key, tmp.value;


CREATE TABLE DEST1(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key, unionsrc.value;

EXPLAIN FROM UNIQUEJOIN PRESERVE src a (a.key), PRESERVE src1 b (b.key), PRESERVE srcpart c (c.key) SELECT a.key, b.key, c.key;

set hive.entity.capture.transform=true;

EXPLAIN
SELECT 
TRANSFORM(a.key, a.value) USING 'cat' AS (tkey, tvalue)
FROM src a join src b
on a.key = b.key;

explain
FROM (
      select key, value from (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2) unionsub
                         UNION all
      select key, value from src s0
                             ) unionsrc
INSERT OVERWRITE TABLE DEST1 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

explain
FROM (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2
                             ) unionsrc
INSERT OVERWRITE TABLE DEST1 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;
