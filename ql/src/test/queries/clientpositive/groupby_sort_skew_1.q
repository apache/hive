;

set hive.exec.reducers.max = 10;
set hive.map.groupby.sorted=true;
set hive.groupby.skewindata=true;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.20S)
-- SORT_QUERY_RESULTS

CREATE TABLE T1_n35(key STRING, val STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/bucket_files/000000_0' INTO TABLE T1_n35;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T1_n35 select key, val from T1_n35;

CREATE TABLE outputTbl1_n8(key int, cnt int);

-- The plan should be converted to a map-side group by if the group by key
-- matches the sorted key
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1_n8
SELECT key, count(1) FROM T1_n35 GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1_n8
SELECT key, count(1) FROM T1_n35 GROUP BY key;

SELECT * FROM outputTbl1_n8;

CREATE TABLE outputTbl2_n2(key1 int, key2 string, cnt int);

-- no map-side group by even if the group by key is a superset of sorted key
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl2_n2
SELECT key, val, count(1) FROM T1_n35 GROUP BY key, val;

INSERT OVERWRITE TABLE outputTbl2_n2
SELECT key, val, count(1) FROM T1_n35 GROUP BY key, val;

SELECT * FROM outputTbl2_n2;

-- It should work for sub-queries
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl1_n8
SELECT key, count(1) FROM (SELECT key, val FROM T1_n35) subq1 GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1_n8
SELECT key, count(1) FROM (SELECT key, val FROM T1_n35) subq1 GROUP BY key;

SELECT * FROM outputTbl1_n8;

-- It should work for sub-queries with column aliases
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1_n8
SELECT k, count(1) FROM (SELECT key as k, val as v FROM T1_n35) subq1 GROUP BY k;

INSERT OVERWRITE TABLE outputTbl1_n8
SELECT k, count(1) FROM (SELECT key as k, val as v FROM T1_n35) subq1 GROUP BY k;

SELECT * FROM outputTbl1_n8;

CREATE TABLE outputTbl3_n0(key1 int, key2 int, cnt int);

-- The plan should be converted to a map-side group by if the group by key contains a constant followed
-- by a match to the sorted key
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl3_n0
SELECT 1, key, count(1) FROM T1_n35 GROUP BY 1, key;

INSERT OVERWRITE TABLE outputTbl3_n0
SELECT 1, key, count(1) FROM T1_n35 GROUP BY 1, key;

SELECT * FROM outputTbl3_n0;

CREATE TABLE outputTbl4_n0(key1 int, key2 int, key3 string, cnt int);

-- no map-side group by if the group by key contains a constant followed by another column
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl4_n0
SELECT key, 1, val, count(1) FROM T1_n35 GROUP BY key, 1, val;

INSERT OVERWRITE TABLE outputTbl4_n0
SELECT key, 1, val, count(1) FROM T1_n35 GROUP BY key, 1, val;

SELECT * FROM outputTbl4_n0;

-- no map-side group by if the group by key contains a function
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl3_n0
SELECT key, key + 1, count(1) FROM T1_n35 GROUP BY key, key + 1;

INSERT OVERWRITE TABLE outputTbl3_n0
SELECT key, key + 1, count(1) FROM T1_n35 GROUP BY key, key + 1;

SELECT * FROM outputTbl3_n0;

-- it should not matter what follows the group by
-- test various cases

-- group by followed by another group by
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl1_n8
SELECT key + key, sum(cnt) from
(SELECT key, count(1) as cnt FROM T1_n35 GROUP BY key) subq1
group by key + key;

INSERT OVERWRITE TABLE outputTbl1_n8
SELECT key + key, sum(cnt) from
(SELECT key, count(1) as cnt FROM T1_n35 GROUP BY key) subq1
group by key + key;

SELECT * FROM outputTbl1_n8;

-- group by followed by a union
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl1_n8
SELECT * FROM (
SELECT key, count(1) FROM T1_n35 GROUP BY key
  UNION ALL
SELECT key, count(1) FROM T1_n35 GROUP BY key
) subq1;

INSERT OVERWRITE TABLE outputTbl1_n8
SELECT * FROM (
SELECT key, count(1) FROM T1_n35 GROUP BY key
  UNION ALL
SELECT key, count(1) FROM T1_n35 GROUP BY key
) subq1;

SELECT * FROM outputTbl1_n8;

-- group by followed by a union where one of the sub-queries is map-side group by
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl1_n8
SELECT * FROM (
SELECT key, count(1) FROM T1_n35 GROUP BY key
  UNION ALL
SELECT key + key as key, count(1) FROM T1_n35 GROUP BY key + key
) subq1;

INSERT OVERWRITE TABLE outputTbl1_n8
SELECT * FROM (
SELECT key, count(1) as cnt FROM T1_n35 GROUP BY key
  UNION ALL
SELECT key + key as key, count(1) as cnt FROM T1_n35 GROUP BY key + key
) subq1;

SELECT * FROM outputTbl1_n8;

-- group by followed by a join
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl1_n8
SELECT subq1.key, subq1.cnt+subq2.cnt FROM 
(SELECT key, count(1) as cnt FROM T1_n35 GROUP BY key) subq1
JOIN
(SELECT key, count(1) as cnt FROM T1_n35 GROUP BY key) subq2
ON subq1.key = subq2.key;

INSERT OVERWRITE TABLE outputTbl1_n8
SELECT subq1.key, subq1.cnt+subq2.cnt FROM 
(SELECT key, count(1) as cnt FROM T1_n35 GROUP BY key) subq1
JOIN
(SELECT key, count(1) as cnt FROM T1_n35 GROUP BY key) subq2
ON subq1.key = subq2.key;

SELECT * FROM outputTbl1_n8;

-- group by followed by a join where one of the sub-queries can be performed in the mapper
EXPLAIN EXTENDED 
SELECT * FROM 
(SELECT key, count(1) FROM T1_n35 GROUP BY key) subq1
JOIN
(SELECT key, val, count(1) FROM T1_n35 GROUP BY key, val) subq2
ON subq1.key = subq2.key;

CREATE TABLE T2_n23(key STRING, val STRING)
CLUSTERED BY (key, val) SORTED BY (key, val) INTO 2 BUCKETS STORED AS TEXTFILE;

-- perform an insert to make sure there are 2 files
INSERT OVERWRITE TABLE T2_n23 select key, val from T1_n35;

-- no mapside sort group by if the group by is a prefix of the sorted key
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl1_n8
SELECT key, count(1) FROM T2_n23 GROUP BY key;

INSERT OVERWRITE TABLE outputTbl1_n8
SELECT key, count(1) FROM T2_n23 GROUP BY key;

SELECT * FROM outputTbl1_n8;

-- The plan should be converted to a map-side group by if the group by key contains a constant in between the
-- sorted keys
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl4_n0
SELECT key, 1, val, count(1) FROM T2_n23 GROUP BY key, 1, val;

INSERT OVERWRITE TABLE outputTbl4_n0
SELECT key, 1, val, count(1) FROM T2_n23 GROUP BY key, 1, val;

SELECT * FROM outputTbl4_n0;

CREATE TABLE outputTbl5_n0(key1 int, key2 int, key3 string, key4 int, cnt int);

-- The plan should be converted to a map-side group by if the group by key contains a constant in between the
-- sorted keys followed by anything
EXPLAIN EXTENDED 
INSERT OVERWRITE TABLE outputTbl5_n0
SELECT key, 1, val, 2, count(1) FROM T2_n23 GROUP BY key, 1, val, 2;

INSERT OVERWRITE TABLE outputTbl5_n0
SELECT key, 1, val, 2, count(1) FROM T2_n23 GROUP BY key, 1, val, 2;

SELECT * FROM outputTbl5_n0 
ORDER BY key1, key2, key3, key4;

-- contants from sub-queries should work fine
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl4_n0
SELECT key, constant, val, count(1) from 
(SELECT key, 1 as constant, val from T2_n23)subq
group by key, constant, val;

INSERT OVERWRITE TABLE outputTbl4_n0
SELECT key, constant, val, count(1) from 
(SELECT key, 1 as constant, val from T2_n23)subq
group by key, constant, val;

SELECT * FROM outputTbl4_n0;

-- multiple levels of contants from sub-queries should work fine
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE outputTbl4_n0
select key, constant3, val, count(1) from
(
SELECT key, constant as constant2, val, 2 as constant3 from 
(SELECT key, 1 as constant, val from T2_n23)subq
)subq2
group by key, constant3, val;

INSERT OVERWRITE TABLE outputTbl4_n0
select key, constant3, val, count(1) from
(
SELECT key, constant as constant2, val, 2 as constant3 from 
(SELECT key, 1 as constant, val from T2_n23)subq
)subq2
group by key, constant3, val;

SELECT * FROM outputTbl4_n0;

set hive.map.aggr=true;
set hive.multigroupby.singlereducer=false;
set mapred.reduce.tasks=31;

CREATE TABLE DEST1_n30(key INT, cnt INT);
CREATE TABLE DEST2_n6(key INT, val STRING, cnt INT);

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true; 

EXPLAIN
FROM T2_n23
INSERT OVERWRITE TABLE DEST1_n30 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE DEST2_n6 SELECT key, val, count(1) GROUP BY key, val;

FROM T2_n23
INSERT OVERWRITE TABLE DEST1_n30 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE DEST2_n6 SELECT key, val, count(1) GROUP BY key, val;

select * from DEST1_n30;
select * from DEST2_n6;

-- multi-table insert with a sub-query
EXPLAIN
FROM (select key, val from T2_n23 where key = 8) x
INSERT OVERWRITE TABLE DEST1_n30 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE DEST2_n6 SELECT key, val, count(1) GROUP BY key, val;

FROM (select key, val from T2_n23 where key = 8) x
INSERT OVERWRITE TABLE DEST1_n30 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE DEST2_n6 SELECT key, val, count(1) GROUP BY key, val;

select * from DEST1_n30;
select * from DEST2_n6;
