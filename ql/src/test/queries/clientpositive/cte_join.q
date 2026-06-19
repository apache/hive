CREATE TABLE t1 (a int, b varchar(100));

EXPLAIN AST
SELECT S.a, t1.a, t1.b FROM (
WITH
 sub1 AS (SELECT a, b FROM t1 WHERE b = 'c')
 SELECT sub1.a, sub1.b FROM sub1
) S
JOIN t1 ON S.a = t1.a;

EXPLAIN CBO
SELECT S.a, t1.a, t1.b FROM (
WITH
 sub1 AS (SELECT a, b FROM t1 WHERE b = 'c')
 SELECT sub1.a, sub1.b FROM sub1
) S
JOIN t1 ON S.a = t1.a;

EXPLAIN
SELECT S.a, t1.a, t1.b FROM (
WITH
 sub1 AS (SELECT a, b FROM t1 WHERE b = 'c')
 SELECT sub1.a, sub1.b FROM sub1
) S
JOIN t1 ON S.a = t1.a;

SELECT S.a, t1.a, t1.b FROM (
WITH
 sub1 AS (SELECT a, b FROM t1 WHERE b = 'c')
 SELECT sub1.a, sub1.b FROM sub1
) S
JOIN t1 ON S.a = t1.a;
