--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;
SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.CheckColumnAccessHook;
SET hive.stats.collect.scancols=true;

-- SORT_QUERY_RESULTS
-- This test is used for testing the ColumnAccessAnalyzer

CREATE TABLE T1_n127(key STRING, val STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n127;

CREATE TABLE T2_n75(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3_n29(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T4_n16(key STRING, val STRING) PARTITIONED BY (p STRING);

-- Simple select queries
SELECT key FROM T1_n127;
SELECT key, val FROM T1_n127;
SELECT 1 FROM T1_n127;
SELECT key, val from T4_n16 where p=1;
SELECT val FROM T4_n16 where p=1;
SELECT p, val FROM T4_n16 where p=1;

-- More complicated select queries
EXPLAIN SELECT key FROM (SELECT key, val FROM T1_n127) subq1;
SELECT key FROM (SELECT key, val FROM T1_n127) subq1;
EXPLAIN SELECT k FROM (SELECT key as k, val as v FROM T1_n127) subq1;
SELECT k FROM (SELECT key as k, val as v FROM T1_n127) subq1;
SELECT key + 1 as k FROM T1_n127;
SELECT key + val as k FROM T1_n127;

-- Work with union
EXPLAIN
SELECT * FROM (
SELECT key as c FROM T1_n127
 UNION ALL
SELECT val as c FROM T1_n127
) subq1;

SELECT * FROM (
SELECT key as c FROM T1_n127
 UNION ALL
SELECT val as c FROM T1_n127
) subq1;

EXPLAIN
SELECT * FROM (
SELECT key as c FROM T1_n127
 UNION ALL
SELECT key as c FROM T1_n127
) subq1;

SELECT * FROM (
SELECT key as c FROM T1_n127
 UNION ALL
SELECT key as c FROM T1_n127
) subq1;

-- Work with insert overwrite
FROM T1_n127
INSERT OVERWRITE TABLE T2_n75 SELECT key, count(1) GROUP BY key
INSERT OVERWRITE TABLE T3_n29 SELECT key, sum(val) GROUP BY key;

-- Simple joins
SELECT *
FROM T1_n127 JOIN T2_n75
ON T1_n127.key = T2_n75.key ;

EXPLAIN
SELECT T1_n127.key
FROM T1_n127 JOIN T2_n75
ON T1_n127.key = T2_n75.key;

SELECT T1_n127.key
FROM T1_n127 JOIN T2_n75
ON T1_n127.key = T2_n75.key;

SELECT *
FROM T1_n127 JOIN T2_n75
ON T1_n127.key = T2_n75.key AND T1_n127.val = T2_n75.val;

-- Map join
SELECT /*+ MAPJOIN(a) */ * 
FROM T1_n127 a JOIN T2_n75 b 
ON a.key = b.key;

-- More joins
EXPLAIN
SELECT *
FROM T1_n127 JOIN T2_n75
ON T1_n127.key = T2_n75.key AND T1_n127.val = 3 and T2_n75.val = 3;

SELECT *
FROM T1_n127 JOIN T2_n75
ON T1_n127.key = T2_n75.key AND T1_n127.val = 3 and T2_n75.val = 3;

EXPLAIN
SELECT subq1.val
FROM 
(
  SELECT val FROM T1_n127 WHERE key = 5  
) subq1
JOIN 
(
  SELECT val FROM T2_n75 WHERE key = 6
) subq2 
ON subq1.val = subq2.val;

SELECT subq1.val
FROM 
(
  SELECT val FROM T1_n127 WHERE key = 5  
) subq1
JOIN 
(
  SELECT val FROM T2_n75 WHERE key = 6
) subq2 
ON subq1.val = subq2.val;

-- Join followed by join
EXPLAIN
SELECT *
FROM
(
  SELECT subq1.key as key
  FROM
  (
    SELECT key, val FROM T1_n127
  ) subq1
  JOIN
  (
    SELECT key, 'teststring' as val FROM T2_n75
  ) subq2
  ON subq1.key = subq2.key
) T4_n16
JOIN T3_n29
ON T3_n29.key = T4_n16.key;

SELECT *
FROM
(
  SELECT subq1.key as key
  FROM
  (
    SELECT key, val FROM T1_n127
  ) subq1
  JOIN
  (
    SELECT key, 'teststring' as val FROM T2_n75
  ) subq2
  ON subq1.key = subq2.key
) T4_n16
JOIN T3_n29
ON T3_n29.key = T4_n16.key;

-- for partitioned table
SELECT * FROM srcpart TABLESAMPLE (10 ROWS);
SELECT key,ds FROM srcpart TABLESAMPLE (10 ROWS) WHERE hr='11';
SELECT value FROM srcpart TABLESAMPLE (10 ROWS) WHERE ds='2008-04-08';
