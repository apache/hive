# needed to avoid the simplification of CAST(NULL) into NULL
set hive.cbo.rule.exclusion.regex=ReduceExpressionsRule\(Project\);

CREATE EXTERNAL TABLE t (a string, b string);

INSERT INTO t VALUES ('1000', 'b1');
INSERT INTO t VALUES ('2000', 'b2');

SELECT * FROM (
  SELECT
   a,
   b
  FROM t
   UNION ALL
  SELECT
   a,
   CAST(NULL AS string)
   FROM t) AS t2
WHERE a = 1000;

EXPLAIN CBO
SELECT * FROM (
  SELECT
   a,
   b
  FROM t
   UNION ALL
  SELECT
   a,
   CAST(NULL AS string)
   FROM t) AS t2
WHERE a = 1000;
