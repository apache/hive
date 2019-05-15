set hive.mapred.mode=nonstrict;
SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.CheckTableAccessHook;
SET hive.stats.collect.tablekeys=true;

-- SORT_QUERY_RESULTS
-- This test is used for testing the TableAccessAnalyzer

CREATE TABLE T1_n13(key STRING, val STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n13;

CREATE TABLE T2_n8(key STRING, val STRING) STORED AS TEXTFILE;
CREATE TABLE T3_n4(key STRING, val STRING) STORED AS TEXTFILE;

-- Simple group-by queries
SELECT key, count(1) FROM T1_n13 GROUP BY key;
SELECT key, val, count(1) FROM T1_n13 GROUP BY key, val;

-- With subqueries and column aliases
SELECT key, count(1) FROM (SELECT key, val FROM T1_n13) subq1 GROUP BY key;
SELECT k, count(1) FROM (SELECT key as k, val as v FROM T1_n13) subq1 GROUP BY k;

-- With constants
SELECT 1, key, count(1) FROM T1_n13 GROUP BY 1, key;
SELECT key, 1, val, count(1) FROM T1_n13 GROUP BY key, 1, val;
SELECT key, 1, val, 2, count(1) FROM T1_n13 GROUP BY key, 1, val, 2;

-- no mapping with functions
SELECT key, key + 1, count(1) FROM T1_n13 GROUP BY key, key + 1;

SELECT key + key, sum(cnt) from
(SELECT key, count(1) as cnt FROM T1_n13 GROUP BY key) subq1
group by key + key;

-- group by followed by union
SELECT * FROM (
SELECT key, count(1) as c FROM T1_n13 GROUP BY key
 UNION ALL
SELECT key, count(1) as c FROM T1_n13 GROUP BY key
) subq1;

-- group by followed by a join
SELECT * FROM
(SELECT key, count(1) as c FROM T1_n13 GROUP BY key) subq1
JOIN
(SELECT key, count(1) as c FROM T1_n13 GROUP BY key) subq2
ON subq1.key = subq2.key;

SELECT * FROM
(SELECT key, count(1) as c FROM T1_n13 GROUP BY key) subq1
JOIN
(SELECT key, val, count(1) as c FROM T1_n13 GROUP BY key, val) subq2
ON subq1.key = subq2.key
ORDER BY subq1.key ASC, subq1.c ASC, subq2.key ASC, subq2.val ASC, subq2.c ASC;

-- constants from sub-queries should work fine
SELECT key, constant, val, count(1) from
(SELECT key, 1 as constant, val from T1_n13) subq1
group by key, constant, val;

-- multiple levels of constants from sub-queries should work fine
SELECT key, constant3, val, count(1) FROM
(
  SELECT key, constant AS constant2, val, 2 AS constant3
  FROM
  (
    SELECT key, 1 AS constant, val
    FROM T1_n13
  ) subq
) subq2
GROUP BY key, constant3, val;

-- work with insert overwrite
FROM T1_n13
INSERT OVERWRITE TABLE T2_n8 SELECT key, count(1) GROUP BY key, 1
INSERT OVERWRITE TABLE T3_n4 SELECT key, sum(val) GROUP BY key;

-- simple joins
SELECT *
FROM T1_n13 JOIN T2_n8
ON T1_n13.key = t2_n8.key
ORDER BY T1_n13.key ASC, T1_n13.val ASC;

SELECT *
FROM T1_n13 JOIN T2_n8
ON T1_n13.key = T2_n8.key AND T1_n13.val = T2_n8.val;

-- map join
SELECT /*+ MAPJOIN(a) */ * 
FROM T1_n13 a JOIN T2_n8 b 
ON a.key = b.key;

-- with constant in join condition
SELECT *
FROM T1_n13 JOIN T2_n8
ON T1_n13.key = T2_n8.key AND T1_n13.val = 3 and T2_n8.val = 3;

-- subqueries
SELECT *
FROM 
(
  SELECT val FROM T1_n13 WHERE key = 5  
) subq1
JOIN 
(
  SELECT val FROM T2_n8 WHERE key = 6
) subq2 
ON subq1.val = subq2.val;

SELECT *
FROM
(
  SELECT val FROM T1_n13 WHERE key = 5
) subq1
JOIN
T2_n8
ON subq1.val = T2_n8.val;

-- with column aliases in subqueries
SELECT *
FROM
(
  SELECT val as v FROM T1_n13 WHERE key = 5
) subq1
JOIN
(
  SELECT val FROM T2_n8 WHERE key = 6
) subq2
ON subq1.v = subq2.val;

-- with constants in subqueries
SELECT *
FROM
(
  SELECT key, val FROM T1_n13
) subq1
JOIN
(
  SELECT key, 'teststring' as val FROM T2_n8
) subq2
ON subq1.val = subq2.val AND subq1.key = subq2.key;

-- multiple levels of constants in subqueries
SELECT *
FROM
(
  SELECT key, val from 
  (
    SELECT key, 'teststring' as val from T1_n13
  ) subq1
) subq2
JOIN
(
  SELECT key, val FROM T2_n8
) subq3
ON subq3.val = subq2.val AND subq3.key = subq2.key;

-- no mapping on functions
SELECT *
FROM
(
  SELECT key, val from T1_n13
) subq1
JOIN
(
  SELECT key, val FROM T2_n8
) subq2
ON subq1.val = subq2.val AND subq1.key + 1 = subq2.key;

-- join followed by group by
SELECT subq1.val, COUNT(*)
FROM
(
  SELECT key, val FROM T1_n13
) subq1
JOIN
(
  SELECT key, 'teststring' as val FROM T2_n8
) subq2
ON subq1.val = subq2.val AND subq1.key = subq2.key
GROUP BY subq1.val;

-- join followed by union
SELECT * 
FROM
(
  SELECT subq1.val, COUNT(*)
  FROM
  (
    SELECT key, val FROM T1_n13
  ) subq1
  JOIN
  (
    SELECT key, 'teststring' as val FROM T2_n8 
  ) subq2
  ON subq1.val = subq2.val AND subq1.key = subq2.key
  GROUP BY subq1.val
 UNION ALL
  SELECT val, COUNT(*)
  FROM T3_n4
  GROUP BY val
) subq4;

-- join followed by join
SELECT *
FROM
(
  SELECT subq1.val as val, COUNT(*)
  FROM
  (
    SELECT key, val FROM T1_n13
  ) subq1
  JOIN
  (
    SELECT key, 'teststring' as val FROM T2_n8
  ) subq2
  ON subq1.val = subq2.val AND subq1.key = subq2.key
  GROUP by subq1.val
) T4
JOIN T3_n4
ON T3_n4.val = T4.val;


set hive.cbo.returnpath.hiveop=true;
-- simple joins
SELECT *
FROM T1_n13 JOIN T2_n8
ON T1_n13.key = t2_n8.key
ORDER BY T1_n13.key ASC, T1_n13.val ASC;

SELECT *
FROM T1_n13 JOIN T2_n8
ON T1_n13.key = T2_n8.key AND T1_n13.val = T2_n8.val;


-- group by followed by a join
SELECT * FROM
(SELECT key, count(1) as c FROM T1_n13 GROUP BY key) subq1
JOIN
(SELECT key, count(1) as c FROM T1_n13 GROUP BY key) subq2
ON subq1.key = subq2.key;


set hive.cbo.returnpath.hiveop=false;
