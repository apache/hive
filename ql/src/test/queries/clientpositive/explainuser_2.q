--! qt:dataset:srcpart
--! qt:dataset:src1
--! qt:dataset:src
set hive.strict.checks.bucketing=false;

set hive.explain.user=true;
set hive.metastore.aggregate.stats.cache.enabled=false;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1_n25(key STRING, value STRING, val2 STRING) STORED AS TEXTFILE;

CREATE TABLE ss_n1(k1 STRING,v1 STRING,k2 STRING,v2 STRING,k3 STRING,v3 STRING) STORED AS TEXTFILE;

CREATE TABLE sr(k1 STRING,v1 STRING,k2 STRING,v2 STRING,k3 STRING,v3 STRING) STORED AS TEXTFILE;

CREATE TABLE cs(k1 STRING,v1 STRING,k2 STRING,v2 STRING,k3 STRING,v3 STRING) STORED AS TEXTFILE;

INSERT OVERWRITE TABLE ss_n1
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


ANALYZE TABLE ss_n1 COMPUTE STATISTICS;
ANALYZE TABLE ss_n1 COMPUTE STATISTICS FOR COLUMNS k1,v1,k2,v2,k3,v3;

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
ss_n1.k1,sr.k2,cs.k3,count(ss_n1.v1),count(sr.v2),count(cs.v3)
FROM 
ss_n1,sr,cs,src d1,src d2,src d3,src1,srcpart
where
    ss_n1.k1 = d1.key 
and sr.k1 = d2.key 
and cs.k1 = d3.key 
and ss_n1.k2 = sr.k2
and ss_n1.k3 = sr.k3
and ss_n1.v1 = src1.value
and ss_n1.v2 = srcpart.value
and sr.v2 = cs.v2
and sr.v3 = cs.v3
and ss_n1.v3='ssv3'
and sr.v1='srv1'
and src1.key = 'src1key'
and srcpart.key = 'srcpartkey'
and d1.value = 'd1value'
and d2.value in ('2000Q1','2000Q2','2000Q3')
and d3.value in ('2000Q1','2000Q2','2000Q3')
group by 
ss_n1.k1,sr.k2,cs.k3
order by 
ss_n1.k1,sr.k2,cs.k3
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
set hive.auto.convert.join.noconditionaltask.size=30000;
set hive.stats.fetch.column.stats=false;


EXPLAIN
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08' and z.hr=11);

EXPLAIN
select 
ss_n1.k1,sr.k2,cs.k3,count(ss_n1.v1),count(sr.v2),count(cs.v3)
FROM 
ss_n1,sr,cs,src d1,src d2,src d3,src1,srcpart
where
    ss_n1.k1 = d1.key 
and sr.k1 = d2.key 
and cs.k1 = d3.key 
and ss_n1.k2 = sr.k2
and ss_n1.k3 = sr.k3
and ss_n1.v1 = src1.value
and ss_n1.v2 = srcpart.value
and sr.v2 = cs.v2
and sr.v3 = cs.v3
and ss_n1.v3='ssv3'
and sr.v1='srv1'
and src1.key = 'src1key'
and srcpart.key = 'srcpartkey'
and d1.value = 'd1value'
and d2.value in ('2000Q1','2000Q2','2000Q3')
and d3.value in ('2000Q1','2000Q2','2000Q3')
group by 
ss_n1.k1,sr.k2,cs.k3
order by 
ss_n1.k1,sr.k2,cs.k3
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
set hive.auto.convert.join.noconditionaltask.size=20000;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;

CREATE TABLE srcbucket_mapjoin_n22(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part_n14 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part_n23 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n22 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin_n22 partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n23 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n23 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n23 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n23 partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part_n14 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part_n23;

CREATE TABLE tab_n15(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_n15 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n22;

CREATE TABLE tab2_n7(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab2_n7 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n22;

set hive.convert.join.bucket.mapjoin.tez = false;
set hive.auto.convert.sortmerge.join = true;

set hive.auto.convert.join.noconditionaltask.size=2000;

explain 
select s1.key as key, s1.value as value from tab_n15 s1 join tab_n15 s3 on s1.key=s3.key;

explain 
select s1.key as key, s1.value as value from tab_n15 s1 join tab_n15 s3 on s1.key=s3.key join tab_n15 s2 on s1.value=s2.value;

explain 
select s1.key as key, s1.value as value from tab_n15 s1 join tab2_n7 s3 on s1.key=s3.key;

explain 
select s1.key as key, s1.value as value from tab_n15 s1 join tab2_n7 s3 on s1.key=s3.key join tab2_n7 s2 on s1.value=s2.value;

explain
select count(*) from (select s1.key as key, s1.value as value from tab_n15 s1 join tab_n15 s3 on s1.key=s3.key
UNION  ALL
select s2.key as key, s2.value as value from tab_n15 s2
) a_n19 join tab_part_n14 b_n15 on (a_n19.key = b_n15.key);

explain
select count(*) from (select s1.key as key, s1.value as value from tab_n15 s1 join tab_n15 s3 on s1.key=s3.key join tab_n15 s2 on s1.value=s2.value
UNION  ALL
select s2.key as key, s2.value as value from tab_n15 s2
) a_n19 join tab_part_n14 b_n15 on (a_n19.key = b_n15.key);set hive.explain.user=true;

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

CREATE TABLE a_n19(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE b_n15(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE c_n4(key STRING, value STRING) STORED AS TEXTFILE;

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
INSERT OVERWRITE TABLE a_n19 SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE b_n15 SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE c_n4 SELECT tmp.key, tmp.value;

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
INSERT OVERWRITE TABLE a_n19 SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE b_n15 SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE c_n4 SELECT tmp.key, tmp.value;


CREATE TABLE DEST1_n172(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2_n43(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2) unionsrc_n4
INSERT OVERWRITE TABLE DEST1_n172 SELECT unionsrc_n4.key, COUNT(DISTINCT SUBSTR(unionsrc_n4.value,5)) GROUP BY unionsrc_n4.key
INSERT OVERWRITE TABLE DEST2_n43 SELECT unionsrc_n4.key, unionsrc_n4.value, COUNT(DISTINCT SUBSTR(unionsrc_n4.value,5)) GROUP BY unionsrc_n4.key, unionsrc_n4.value;

EXPLAIN FROM UNIQUEJOIN PRESERVE src a_n19 (a_n19.key), PRESERVE src1 b_n15 (b_n15.key), PRESERVE srcpart c_n4 (c_n4.key) SELECT a_n19.key, b_n15.key, c_n4.key;

set hive.entity.capture.transform=true;

EXPLAIN
SELECT 
TRANSFORM(a_n19.key, a_n19.value) USING 'cat' AS (tkey, tvalue)
FROM src a_n19 join src b_n15
on a_n19.key = b_n15.key;

explain
FROM (
      select key, value from (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2) unionsub_n15
                         UNION all
      select key, value from src s0
                             ) unionsrc_n4
INSERT OVERWRITE TABLE DEST1_n172 SELECT unionsrc_n4.key, COUNT(DISTINCT SUBSTR(unionsrc_n4.value,5)) GROUP BY unionsrc_n4.key
INSERT OVERWRITE TABLE DEST2_n43 SELECT unionsrc_n4.key, unionsrc_n4.value, COUNT(DISTINCT SUBSTR(unionsrc_n4.value,5)) 
GROUP BY unionsrc_n4.key, unionsrc_n4.value;

explain
FROM (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2
                             ) unionsrc_n4
INSERT OVERWRITE TABLE DEST1_n172 SELECT unionsrc_n4.key, COUNT(DISTINCT SUBSTR(unionsrc_n4.value,5)) GROUP BY unionsrc_n4.key
INSERT OVERWRITE TABLE DEST2_n43 SELECT unionsrc_n4.key, unionsrc_n4.value, COUNT(DISTINCT SUBSTR(unionsrc_n4.value,5)) 
GROUP BY unionsrc_n4.key, unionsrc_n4.value;
