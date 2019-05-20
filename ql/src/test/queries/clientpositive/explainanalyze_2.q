--! qt:dataset:srcpart
--! qt:dataset:src1
--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
set hive.map.aggr=false;

set hive.strict.checks.bucketing=false;
set hive.explain.user=true;

explain analyze
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union select * from src)z ON (x.value = z.value)
union
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN (select * from src1 union select * from src)z ON (x.value = z.value);

explain analyze
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

CREATE TABLE srcbucket_mapjoin_n11(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part_n7 (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part_n11 (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_n11 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj1/000001_0' INTO TABLE srcbucket_mapjoin_n11 partition(ds='2008-04-08');

load data local inpath '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_n11 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_n11 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_n11 partition(ds='2008-04-08');
load data local inpath '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_n11 partition(ds='2008-04-08');



set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part_n7 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part_n11;

CREATE TABLE tab_n6(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab_n6 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n11;

CREATE TABLE tab2_n3(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab2_n3 partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_n11;

set hive.convert.join.bucket.mapjoin.tez = false;
set hive.auto.convert.sortmerge.join = true;

set hive.auto.convert.join.noconditionaltask.size=500;

explain analyze 
select s1.key as key, s1.value as value from tab_n6 s1 join tab_n6 s3 on s1.key=s3.key;

explain analyze 
select s1.key as key, s1.value as value from tab_n6 s1 join tab_n6 s3 on s1.key=s3.key join tab_n6 s2 on s1.value=s2.value;

explain analyze 
select s1.key as key, s1.value as value from tab_n6 s1 join tab2_n3 s3 on s1.key=s3.key;

explain analyze 
select s1.key as key, s1.value as value from tab_n6 s1 join tab2_n3 s3 on s1.key=s3.key join tab2_n3 s2 on s1.value=s2.value;

explain analyze
select count(*) from (select s1.key as key, s1.value as value from tab_n6 s1 join tab_n6 s3 on s1.key=s3.key
UNION  ALL
select s2.key as key, s2.value as value from tab_n6 s2
) a_n14 join tab_part_n7 b_n10 on (a_n14.key = b_n10.key);

explain analyze
select count(*) from (select s1.key as key, s1.value as value from tab_n6 s1 join tab_n6 s3 on s1.key=s3.key join tab_n6 s2 on s1.value=s2.value
UNION  ALL
select s2.key as key, s2.value as value from tab_n6 s2
) a_n14 join tab_part_n7 b_n10 on (a_n14.key = b_n10.key);

CREATE TABLE a_n14(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE b_n10(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE c_n3(key STRING, value STRING) STORED AS TEXTFILE;

explain analyze
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
INSERT OVERWRITE TABLE a_n14 SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE b_n10 SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE c_n3 SELECT tmp.key, tmp.value;

explain analyze
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
INSERT OVERWRITE TABLE a_n14 SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE b_n10 SELECT tmp.key, tmp.value
INSERT OVERWRITE TABLE c_n3 SELECT tmp.key, tmp.value;


CREATE TABLE DEST1_n105(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2_n29(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

explain analyze 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION DISTINCT  
      select s2.key as key, s2.value as value from src s2) unionsrc_n3
INSERT OVERWRITE TABLE DEST1_n105 SELECT unionsrc_n3.key, COUNT(DISTINCT SUBSTR(unionsrc_n3.value,5)) GROUP BY unionsrc_n3.key
INSERT OVERWRITE TABLE DEST2_n29 SELECT unionsrc_n3.key, unionsrc_n3.value, COUNT(DISTINCT SUBSTR(unionsrc_n3.value,5)) GROUP BY unionsrc_n3.key, unionsrc_n3.value;

explain analyze FROM UNIQUEJOIN PRESERVE src a_n14 (a_n14.key), PRESERVE src1 b_n10 (b_n10.key), PRESERVE srcpart c_n3 (c_n3.key) SELECT a_n14.key, b_n10.key, c_n3.key;


explain analyze
FROM (
      select key, value from (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2) unionsub_n10
                         UNION all
      select key, value from src s0
                             ) unionsrc_n3
INSERT OVERWRITE TABLE DEST1_n105 SELECT unionsrc_n3.key, COUNT(DISTINCT SUBSTR(unionsrc_n3.value,5)) GROUP BY unionsrc_n3.key
INSERT OVERWRITE TABLE DEST2_n29 SELECT unionsrc_n3.key, unionsrc_n3.value, COUNT(DISTINCT SUBSTR(unionsrc_n3.value,5)) 
GROUP BY unionsrc_n3.key, unionsrc_n3.value;

explain analyze
FROM (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2
                             ) unionsrc_n3
INSERT OVERWRITE TABLE DEST1_n105 SELECT unionsrc_n3.key, COUNT(DISTINCT SUBSTR(unionsrc_n3.value,5)) GROUP BY unionsrc_n3.key
INSERT OVERWRITE TABLE DEST2_n29 SELECT unionsrc_n3.key, unionsrc_n3.value, COUNT(DISTINCT SUBSTR(unionsrc_n3.value,5)) 
GROUP BY unionsrc_n3.key, unionsrc_n3.value;
