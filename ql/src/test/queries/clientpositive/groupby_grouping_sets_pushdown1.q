SET hive.cbo.enable=false;

CREATE TABLE T1(a STRING, b STRING, s BIGINT);
INSERT OVERWRITE TABLE T1 VALUES ('aaa', 'bbb', 123456);

EXPLAIN EXTENDED SELECT * FROM (
SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((), (a), (b), (a, b))
) t WHERE a IS NOT NULL;

EXPLAIN EXTENDED SELECT * FROM (
SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((a), (a, b))
) t WHERE a IS NOT NULL;

EXPLAIN EXTENDED SELECT * FROM (
SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((a), (a, b))
HAVING sum(s) > 100
) t WHERE a IS NOT NULL AND b IS NOT NULL;

EXPLAIN EXTENDED SELECT * FROM (
SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((a), (a, b))
HAVING sum(s) > 100
) t WHERE a IS NOT NULL OR b IS NOT NULL;

SELECT * FROM (
SELECT a, b, sum(s)
FROM T1
GROUP BY a, b WITH CUBE
) t WHERE a IS NOT NULL OR b IS NOT NULL;

SELECT * FROM (
SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((a), (a, b))
) t WHERE b IS NULL;

EXPLAIN EXTENDED SELECT * FROM (
SELECT upper(a) x, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((a), (a, b))
) t WHERE x in ("AAA", "BBB");

SELECT * FROM (
SELECT upper(a) x, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((a), (a, b))
) t WHERE x in ('AAA', 'BBB');

EXPLAIN EXTENDED SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((a), (a, b))
HAVING upper(a) = 'AAA' AND 1 != 1;

SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((a), (a, b))
HAVING upper(a) = 'AAA' AND 1 != 1;

EXPLAIN EXTENDED SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((), (a), (a, b))
HAVING upper(a) = 'AAA' AND sum(s) > 100;

SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((), (a), (a, b))
HAVING upper(a) = 'AAA' AND sum(s) > 100;

EXPLAIN EXTENDED SELECT upper(a), b, sum(s)
FROM T1
GROUP BY upper(a), b GROUPING SETS ((upper(a)), (upper(a), b))
HAVING upper(a) = 'AAA' AND sum(s) > 100;

SELECT upper(a), b, sum(s)
FROM T1
GROUP BY upper(a), b GROUPING SETS ((upper(a)), (upper(a), b))
HAVING upper(a) = 'AAA' AND sum(s) > 100;

EXPLAIN EXTENDED SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((b), (a, b))
HAVING sum(s) > 100 and a IS NOT NULL AND upper(b) = 'BBB';

SELECT a, b, sum(s)
FROM T1
GROUP BY a, b GROUPING SETS ((b), (a, b))
HAVING sum(s) > 100 and a IS NOT NULL AND upper(b) = 'BBB';
