set hive.mapred.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.spark.use.ts.stats.for.mapjoin=true;
set hive.auto.convert.join.noconditionaltask.size=4000;
-- SORT_QUERY_RESULTS

EXPLAIN
SELECT src1.key, src2.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
WHERE src1.key = 97;

SELECT src1.key, src2.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
WHERE src1.key = 97;

set hive.auto.convert.join.noconditionaltask.size=8000;

-- This is copied from auto_join2. Without the configuration both joins are mapjoins,
-- but with the configuration on, Hive should not turn the second join into mapjoin since it
-- has a upstream reduce sink.

CREATE TABLE dest(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest SELECT src1.key, src3.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest SELECT src1.key, src3.value;

SELECT sum(hash(dest.key,dest.value)) FROM dest;


-- Test for HIVE-16698, for the case of UNION + MAPJOIN

set hive.auto.convert.join.noconditionaltask.size=16;

CREATE TABLE a (c1 STRING, c2 INT);
CREATE TABLE b (c3 STRING, c4 INT);
CREATE TABLE c (c1 STRING, c2 INT);
CREATE TABLE d (c3 STRING, c4 INT);
CREATE TABLE e (c5 STRING, c6 INT);
INSERT INTO TABLE a VALUES ("a1", 1), ("a2", 2), ("a3", 3), ("a4", 4), ("a5", 5), ("a6", 6), ("a7", 7);
INSERT INTO TABLE b VALUES ("b1", 1), ("b2", 2), ("b3", 3), ("b4", 4);
INSERT INTO TABLE c VALUES ("c1", 1), ("c2", 2), ("c3", 3), ("c4", 4), ("c5", 5), ("c6", 6), ("c7", 7);
INSERT INTO TABLE d VALUES ("d1", 1), ("d2", 2), ("d3", 3), ("d4", 4);
INSERT INTO TABLE e VALUES ("d1", 1), ("d2", 2);

EXPLAIN
WITH t1 AS (
SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3 FROM a JOIN b ON a.c2 = b.c4
),
t2 AS (
SELECT c.c1 AS c1, c.c2 AS c2, d.c3 AS c3 FROM c JOIN d ON c.c2 = d.c4
),
t3 AS (
SELECT * FROM t1 UNION ALL SELECT * FROM t2
),
t4 AS (
SELECT t3.c1, t3.c3, t5.c5 FROM t3 JOIN e AS t5 ON t3.c2 = t5.c6
)
SELECT * FROM t4;

WITH t1 AS (
SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3 FROM a JOIN b ON a.c2 = b.c4
),
t2 AS (
SELECT c.c1 AS c1, c.c2 AS c2, d.c3 AS c3 FROM c JOIN d ON c.c2 = d.c4
),
t3 AS (
SELECT * FROM t1 UNION ALL SELECT * FROM t2
),
t4 AS (
SELECT t3.c1, t3.c3, t5.c5 FROM t3 JOIN e AS t5 ON t3.c2 = t5.c6
)
SELECT * FROM t4;
