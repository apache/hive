CREATE EXTERNAL TABLE t (a string, b string);

INSERT INTO t VALUES ('1000', 'b1');
INSERT INTO t VALUES ('1001', 'b1');
INSERT INTO t VALUES ('1002', 'b1');
INSERT INTO t VALUES ('2000', 'b2');

SELECT * FROM (
  SELECT
   a,
   b
  FROM t
   UNION ALL
  SELECT
   a,
   b
   FROM t
   WHERE a = '1001'
    UNION ALL
   SELECT
   a,
   b
   FROM t
   WHERE a = '1002') AS t2
WHERE a = '1000';

EXPLAIN CBO
SELECT * FROM (
  SELECT
   a,
   b
  FROM t
   UNION ALL
  SELECT
   a,
   b
   FROM t
   WHERE a = '1001'
    UNION ALL
   SELECT
   a,
   b
   FROM t
   WHERE a = '1002') AS t2
WHERE a = '1000';
