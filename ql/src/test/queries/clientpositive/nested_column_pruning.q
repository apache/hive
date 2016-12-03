set hive.fetch.task.conversion = none;

-- First, create source tables
DROP TABLE IF EXISTS dummy;
CREATE TABLE dummy (i int);
INSERT INTO TABLE dummy VALUES (42);

DROP TABLE IF EXISTS nested_tbl_1;
CREATE TABLE nested_tbl_1 (
  a int,
  s1 struct<f1: boolean, f2: string, f3: struct<f4: int, f5: double>, f6: int>,
  s2 struct<f7: string, f8: struct<f9 : boolean, f10: array<int>, f11: map<string, boolean>>>,
  s3 struct<f12: array<struct<f13:string, f14:int>>>
) STORED AS PARQUET;

INSERT INTO TABLE nested_tbl_1 SELECT
  1, named_struct('f1', false, 'f2', 'foo', 'f3', named_struct('f4', 4, 'f5', cast(5.0 as double)), 'f6', 4),
  named_struct('f7', 'f7', 'f8', named_struct('f9', true, 'f10', array(10, 11), 'f11', map('key1', true, 'key2', false))),
  named_struct('f12', array(named_struct('f13', 'foo', 'f14', 14), named_struct('f13', 'bar', 'f14', 28)))
FROM dummy;

DROP TABLE IF EXISTS nested_tbl_2;
CREATE TABLE nested_tbl_2 LIKE nested_tbl_1;

INSERT INTO TABLE nested_tbl_2 SELECT
  2, named_struct('f1', true, 'f2', 'bar', 'f3', named_struct('f4', 4, 'f5', cast(6.5 as double)), 'f6', 4),
  named_struct('f7', 'f72', 'f8', named_struct('f9', false, 'f10', array(20, 22), 'f11', map('key3', true, 'key4', false))),
  named_struct('f12', array(named_struct('f13', 'bar', 'f14', 28), named_struct('f13', 'foo', 'f14', 56)))
FROM dummy;

-- Testing only select statements

EXPLAIN SELECT a FROM nested_tbl_1;
SELECT a FROM nested_tbl_1;

EXPLAIN SELECT s1.f1 FROM nested_tbl_1;
SELECT s1.f1 FROM nested_tbl_1;

EXPLAIN SELECT s1.f1, s1.f2 FROM nested_tbl_1;
SELECT s1.f1, s1.f2 FROM nested_tbl_1;

-- In this case 's1.f3' and 's1.f3.f4' should be merged
EXPLAIN SELECT s1.f3, s1.f3.f4 FROM nested_tbl_1;
SELECT s1.f3, s1.f3.f4 FROM nested_tbl_1;

-- Testing select array and index shifting
EXPLAIN SELECT s1.f3.f5 FROM nested_tbl_1;
SELECT s1.f3.f5 FROM nested_tbl_1;

-- Testing select from multiple structs
EXPLAIN SELECT s1.f3.f4, s2.f8.f9 FROM nested_tbl_1;
SELECT s1.f3.f4, s2.f8.f9 FROM nested_tbl_1;


-- Testing select with filter

EXPLAIN SELECT s1.f2 FROM nested_tbl_1 WHERE s1.f1 = FALSE;
SELECT s1.f2 FROM nested_tbl_1 WHERE s1.f1 = FALSE;

EXPLAIN SELECT s1.f3.f5 FROM nested_tbl_1 WHERE s1.f3.f4 = 4;
SELECT s1.f3.f5 FROM nested_tbl_1 WHERE s1.f3.f4 = 4;

EXPLAIN SELECT s2.f8 FROM nested_tbl_1 WHERE s1.f2 = 'foo' AND size(s2.f8.f10) > 1 AND s2.f8.f11['key1'] = TRUE;
SELECT s2.f8 FROM nested_tbl_1 WHERE s1.f2 = 'foo' AND size(s2.f8.f10) > 1 AND s2.f8.f11['key1'] = TRUE;


-- Testing lateral view

EXPLAIN SELECT col1, col2 FROM nested_tbl_1
LATERAL VIEW explode(s2.f8.f10) tbl1 AS col1
LATERAL VIEW explode(s3.f12) tbl2 AS col2;
SELECT col1, col2 FROM nested_tbl_1
LATERAL VIEW explode(s2.f8.f10) tbl1 AS col1
LATERAL VIEW explode(s3.f12) tbl2 AS col2;


-- Testing UDFs
EXPLAIN SELECT pmod(s2.f8.f10[1], s1.f3.f4) FROM nested_tbl_1;
SELECT pmod(s2.f8.f10[1], s1.f3.f4) FROM nested_tbl_1;


-- Testing aggregations

EXPLAIN SELECT s1.f3.f5, count(s1.f3.f4) FROM nested_tbl_1 GROUP BY s1.f3.f5;
SELECT s1.f3.f5, count(s1.f3.f4) FROM nested_tbl_1 GROUP BY s1.f3.f5;

EXPLAIN SELECT s1.f3, count(s1.f3.f4) FROM nested_tbl_1 GROUP BY s1.f3;
SELECT s1.f3, count(s1.f3.f4) FROM nested_tbl_1 GROUP BY s1.f3;

EXPLAIN SELECT s1.f3, count(s1.f3.f4) FROM nested_tbl_1 GROUP BY s1.f3 ORDER BY s1.f3;
SELECT s1.f3, count(s1.f3.f4) FROM nested_tbl_1 GROUP BY s1.f3 ORDER BY s1.f3;


-- Testing joins

EXPLAIN SELECT t1.s1.f3.f5, t2.s2.f8
FROM nested_tbl_1 t1 JOIN nested_tbl_2 t2
ON t1.s1.f3.f4 = t2.s1.f6
WHERE t2.s2.f8.f9 == FALSE;
SELECT t1.s1.f3.f5, t2.s2.f8
FROM nested_tbl_1 t1 JOIN nested_tbl_2 t2
ON t1.s1.f3.f4 = t2.s1.f6
WHERE t2.s2.f8.f9 == FALSE;

EXPLAIN SELECT t1.s1.f3.f5, t2.s2.f8
FROM nested_tbl_1 t1 JOIN nested_tbl_1 t2
ON t1.s1.f3.f4 = t2.s1.f6
WHERE t2.s2.f8.f9 == TRUE;
SELECT t1.s1.f3.f5, t2.s2.f8
FROM nested_tbl_1 t1 JOIN nested_tbl_1 t2
ON t1.s1.f3.f4 = t2.s1.f6
WHERE t2.s2.f8.f9 == TRUE;
