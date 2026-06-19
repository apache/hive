--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

-- This tests various union queries which have columns on one side of the query
-- being of double type and those on the other side another

CREATE TABLE t1_n70 AS SELECT * FROM src WHERE key < 10;
CREATE TABLE t2_n43 AS SELECT * FROM src WHERE key < 10;

-- Test simple union with double
EXPLAIN
SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key FROM t1_n70
UNION ALL
SELECT CAST(key AS BIGINT) AS key FROM t2_n43) a;

SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key FROM t1_n70
UNION ALL
SELECT CAST(key AS BIGINT) AS key FROM t2_n43) a
;

-- Test union with join on the left
EXPLAIN
SELECT * FROM 
(SELECT CAST(a.key AS BIGINT) AS key FROM t1_n70 a JOIN t2_n43 b ON a.key = b.key
UNION ALL
SELECT CAST(key AS DOUBLE) AS key FROM t2_n43) a
;

SELECT * FROM 
(SELECT CAST(a.key AS BIGINT) AS key FROM t1_n70 a JOIN t2_n43 b ON a.key = b.key
UNION ALL
SELECT CAST(key AS DOUBLE) AS key FROM t2_n43) a
;

-- Test union with join on the right
EXPLAIN
SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key FROM t2_n43
UNION ALL
SELECT CAST(a.key AS BIGINT) AS key FROM t1_n70 a JOIN t2_n43 b ON a.key = b.key) a
;

SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key FROM t2_n43
UNION ALL
SELECT CAST(a.key AS BIGINT) AS key FROM t1_n70 a JOIN t2_n43 b ON a.key = b.key) a
;

-- Test union with join on the left selecting multiple columns
EXPLAIN
SELECT * FROM 
(SELECT CAST(a.key AS BIGINT) AS key, CAST(b.key AS CHAR(20)) AS value FROM t1_n70 a JOIN t2_n43 b ON a.key = b.key
UNION ALL
SELECT CAST(key AS DOUBLE) AS key, CAST(key AS STRING) AS value FROM t2_n43) a
;

SELECT * FROM 
(SELECT CAST(a.key AS BIGINT) AS key, CAST(b.key AS VARCHAR(20)) AS value FROM t1_n70 a JOIN t2_n43 b ON a.key = b.key
UNION ALL
SELECT CAST(key AS DOUBLE) AS key, CAST(key AS STRING) AS value FROM t2_n43) a
;

-- Test union with join on the right selecting multiple columns
EXPLAIN
SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key, CAST(key AS STRING) AS value FROM t2_n43
UNION ALL
SELECT CAST(a.key AS BIGINT) AS key, CAST(b.key AS CHAR(20)) AS value FROM t1_n70 a JOIN t2_n43 b ON a.key = b.key) a
;

SELECT * FROM 
(SELECT CAST(key AS DOUBLE) AS key, CAST(key AS STRING) AS value FROM t2_n43
UNION ALL
SELECT CAST(a.key AS BIGINT) AS key, CAST(b.key AS VARCHAR(20)) AS value FROM t1_n70 a JOIN t2_n43 b ON a.key = b.key) a
;
