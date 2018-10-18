-- Suppress vectorization due to known bug.  See HIVE-19016.
set hive.vectorized.execution.enabled=false;
set hive.test.vectorized.execution.enabled.override=none;

set hive.fetch.task.conversion = none;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.strict.checks.cartesian.product=false;

-- First, create source tables
DROP TABLE IF EXISTS dummy_n5;
CREATE TABLE dummy_n5 (i int);
INSERT INTO TABLE dummy_n5 VALUES (42);

DROP TABLE IF EXISTS nested_tbl_1_n1;
CREATE TABLE nested_tbl_1_n1 (
  a int,
  s1 struct<f1: boolean, f2: string, f3: struct<f4: int, f5: double>, f6: int>,
  s2 struct<f7: string, f8: struct<f9 : boolean, f10: array<int>, f11: map<string, boolean>>>,
  s3 struct<f12: array<struct<f13:string, f14:int>>>,
  s4 map<string, struct<f15:int>>,
  s5 struct<f16: array<struct<f17:string, f18:struct<f19:int>>>>,
  s6 map<string, struct<f20:array<struct<f21:struct<f22:int>>>>>
) STORED AS PARQUET;

INSERT INTO TABLE nested_tbl_1_n1 SELECT
  1, named_struct('f1', false, 'f2', 'foo', 'f3', named_struct('f4', 4, 'f5', cast(5.0 as double)), 'f6', 4),
  named_struct('f7', 'f7', 'f8', named_struct('f9', true, 'f10', array(10, 11), 'f11', map('key1', true, 'key2', false))),
  named_struct('f12', array(named_struct('f13', 'foo', 'f14', 14), named_struct('f13', 'bar', 'f14', 28))),
  map('key1', named_struct('f15', 1), 'key2', named_struct('f15', 2)),
  named_struct('f16', array(named_struct('f17', 'foo', 'f18', named_struct('f19', 14)), named_struct('f17', 'bar', 'f18', named_struct('f19', 28)))),
  map('key1', named_struct('f20', array(named_struct('f21', named_struct('f22', 1)))),
      'key2', named_struct('f20', array(named_struct('f21', named_struct('f22', 2)))))
FROM dummy_n5;

DROP TABLE IF EXISTS nested_tbl_2_n1;
CREATE TABLE nested_tbl_2_n1 LIKE nested_tbl_1_n1;

INSERT INTO TABLE nested_tbl_2_n1 SELECT
  2, named_struct('f1', true, 'f2', 'bar', 'f3', named_struct('f4', 4, 'f5', cast(6.5 as double)), 'f6', 4),
  named_struct('f7', 'f72', 'f8', named_struct('f9', false, 'f10', array(20, 22), 'f11', map('key3', true, 'key4', false))),
  named_struct('f12', array(named_struct('f13', 'bar', 'f14', 28), named_struct('f13', 'foo', 'f14', 56))),
  map('key3', named_struct('f15', 3), 'key4', named_struct('f15', 4)),
  named_struct('f16', array(named_struct('f17', 'bar', 'f18', named_struct('f19', 28)), named_struct('f17', 'foo', 'f18', named_struct('f19', 56)))),
  map('key3', named_struct('f20', array(named_struct('f21', named_struct('f22', 3)))),
      'key4', named_struct('f20', array(named_struct('f21', named_struct('f22', 4)))))
FROM dummy_n5;

-- Testing only select statements

EXPLAIN SELECT a FROM nested_tbl_1_n1;
SELECT a FROM nested_tbl_1_n1;

EXPLAIN SELECT s1.f1 FROM nested_tbl_1_n1;
SELECT s1.f1 FROM nested_tbl_1_n1;

EXPLAIN SELECT s1.f1, s1.f2 FROM nested_tbl_1_n1;
SELECT s1.f1, s1.f2 FROM nested_tbl_1_n1;

-- In this case 's1.f3' and 's1.f3.f4' should be merged
EXPLAIN SELECT s1.f3, s1.f3.f4 FROM nested_tbl_1_n1;
SELECT s1.f3, s1.f3.f4 FROM nested_tbl_1_n1;

-- Testing select array and index shifting
EXPLAIN SELECT s1.f3.f5 FROM nested_tbl_1_n1;
SELECT s1.f3.f5 FROM nested_tbl_1_n1;

-- Testing select from multiple structs
EXPLAIN SELECT s1.f3.f4, s2.f8.f9 FROM nested_tbl_1_n1;
SELECT s1.f3.f4, s2.f8.f9 FROM nested_tbl_1_n1;


-- Testing select with filter

EXPLAIN SELECT s1.f2 FROM nested_tbl_1_n1 WHERE s1.f1 = FALSE;
SELECT s1.f2 FROM nested_tbl_1_n1 WHERE s1.f1 = FALSE;

EXPLAIN SELECT s1.f3.f5 FROM nested_tbl_1_n1 WHERE s1.f3.f4 = 4;
SELECT s1.f3.f5 FROM nested_tbl_1_n1 WHERE s1.f3.f4 = 4;

EXPLAIN SELECT s2.f8 FROM nested_tbl_1_n1 WHERE s1.f2 = 'foo' AND size(s2.f8.f10) > 1 AND s2.f8.f11['key1'] = TRUE;
SELECT s2.f8 FROM nested_tbl_1_n1 WHERE s1.f2 = 'foo' AND size(s2.f8.f10) > 1 AND s2.f8.f11['key1'] = TRUE;


-- Testing lateral view

EXPLAIN SELECT col1, col2 FROM nested_tbl_1_n1
LATERAL VIEW explode(s2.f8.f10) tbl1 AS col1
LATERAL VIEW explode(s3.f12) tbl2 AS col2;
SELECT col1, col2 FROM nested_tbl_1_n1
LATERAL VIEW explode(s2.f8.f10) tbl1 AS col1
LATERAL VIEW explode(s3.f12) tbl2 AS col2;


-- Testing UDFs
EXPLAIN SELECT pmod(s2.f8.f10[1], s1.f3.f4) FROM nested_tbl_1_n1;
SELECT pmod(s2.f8.f10[1], s1.f3.f4) FROM nested_tbl_1_n1;


-- Testing aggregations

EXPLAIN SELECT s1.f3.f5, count(s1.f3.f4) FROM nested_tbl_1_n1 GROUP BY s1.f3.f5;
SELECT s1.f3.f5, count(s1.f3.f4) FROM nested_tbl_1_n1 GROUP BY s1.f3.f5;

EXPLAIN SELECT s1.f3, count(s1.f3.f4) FROM nested_tbl_1_n1 GROUP BY s1.f3;
SELECT s1.f3, count(s1.f3.f4) FROM nested_tbl_1_n1 GROUP BY s1.f3;

EXPLAIN SELECT s1.f3, count(s1.f3.f4) FROM nested_tbl_1_n1 GROUP BY s1.f3 ORDER BY s1.f3;
SELECT s1.f3, count(s1.f3.f4) FROM nested_tbl_1_n1 GROUP BY s1.f3 ORDER BY s1.f3;


-- Testing joins

EXPLAIN SELECT t1.s1.f3.f5, t2.s2.f8
FROM nested_tbl_1_n1 t1 JOIN nested_tbl_2_n1 t2
ON t1.s1.f3.f4 = t2.s1.f6
WHERE t2.s2.f8.f9 == FALSE;
SELECT t1.s1.f3.f5, t2.s2.f8
FROM nested_tbl_1_n1 t1 JOIN nested_tbl_2_n1 t2
ON t1.s1.f3.f4 = t2.s1.f6
WHERE t2.s2.f8.f9 == FALSE;

EXPLAIN SELECT t1.s1.f3.f5, t2.s2.f8
FROM nested_tbl_1_n1 t1 JOIN nested_tbl_1_n1 t2
ON t1.s1.f3.f4 = t2.s1.f6
WHERE t2.s2.f8.f9 == TRUE;
SELECT t1.s1.f3.f5, t2.s2.f8
FROM nested_tbl_1_n1 t1 JOIN nested_tbl_1_n1 t2
ON t1.s1.f3.f4 = t2.s1.f6
WHERE t2.s2.f8.f9 == TRUE;

EXPLAIN SELECT t1.s1.f3.f5
FROM nested_tbl_1_n1 t1 LEFT SEMI JOIN nested_tbl_1_n1 t2
ON t1.s1.f3.f4 = t2.s1.f6 AND t2.s2.f8.f9 == TRUE;
SELECT t1.s1.f3.f5
FROM nested_tbl_1_n1 t1 LEFT SEMI JOIN nested_tbl_1_n1 t2
ON t1.s1.f3.f4 = t2.s1.f6 AND t2.s2.f8.f9 == TRUE;

EXPLAIN SELECT t1.s1.f3.f5
FROM nested_tbl_1_n1 t1 LEFT SEMI JOIN nested_tbl_1_n1 t2
ON t1.s1.f1 <> t2.s2.f8.f9;
SELECT t1.s1.f3.f5
FROM nested_tbl_1_n1 t1 LEFT SEMI JOIN nested_tbl_1_n1 t2
ON t1.s1.f1 <> t2.s2.f8.f9;

EXPLAIN SELECT t1.s1.f3.f5
FROM nested_tbl_1_n1 t1 LEFT SEMI JOIN nested_tbl_1_n1 t2
ON t1.s1.f3.f4 = t2.s1.f6 AND t1.s1.f1 <> t2.s2.f8.f9;
SELECT t1.s1.f3.f5
FROM nested_tbl_1_n1 t1 LEFT SEMI JOIN nested_tbl_1_n1 t2
ON t1.s1.f3.f4 = t2.s1.f6 AND t1.s1.f1 <> t2.s2.f8.f9;

-- Testing insert with aliases

DROP TABLE IF EXISTS nested_tbl_3_n1;
CREATE TABLE nested_tbl_3_n1 (f1 boolean, f2 string) PARTITIONED BY (f3 int) STORED AS PARQUET;

INSERT OVERWRITE TABLE nested_tbl_3_n1 PARTITION(f3)
SELECT s1.f1 AS f1, S1.f2 AS f2, s1.f6 AS f3
FROM nested_tbl_1_n1;

SELECT * FROM nested_tbl_3_n1;

-- Testing select struct field from elements in array or map

EXPLAIN
SELECT count(s1.f6), s3.f12[0].f14
FROM nested_tbl_1_n1
GROUP BY s3.f12[0].f14;

SELECT count(s1.f6), s3.f12[0].f14
FROM nested_tbl_1_n1
GROUP BY s3.f12[0].f14;

EXPLAIN
SELECT count(s1.f6), s4['key1'].f15
FROM nested_tbl_1_n1
GROUP BY s4['key1'].f15;

SELECT count(s1.f6), s4['key1'].f15
FROM nested_tbl_1_n1
GROUP BY s4['key1'].f15;

EXPLAIN
SELECT count(s1.f6), s5.f16[0].f18.f19
FROM nested_tbl_1_n1
GROUP BY s5.f16[0].f18.f19;

SELECT count(s1.f6), s5.f16[0].f18.f19
FROM nested_tbl_1_n1
GROUP BY s5.f16[0].f18.f19;

EXPLAIN
SELECT count(s1.f6), s5.f16.f18.f19
FROM nested_tbl_1_n1
GROUP BY s5.f16.f18.f19;

SELECT count(s1.f6), s5.f16.f18.f19
FROM nested_tbl_1_n1
GROUP BY s5.f16.f18.f19;

EXPLAIN
SELECT count(s1.f6), s6['key1'].f20[0].f21.f22
FROM nested_tbl_1_n1
GROUP BY s6['key1'].f20[0].f21.f22;

SELECT count(s1.f6), s6['key1'].f20[0].f21.f22
FROM nested_tbl_1_n1
GROUP BY s6['key1'].f20[0].f21.f22;

EXPLAIN
SELECT count(s1.f6), s6['key1'].f20.f21.f22
FROM nested_tbl_1_n1
GROUP BY s6['key1'].f20.f21.f22;

SELECT count(s1.f6), s6['key1'].f20.f21.f22
FROM nested_tbl_1_n1
GROUP BY s6['key1'].f20.f21.f22;
