-- SORT_QUERY_RESULTS

explain
SELECT *
FROM (
  SELECT 1 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION ALL
  SELECT 2 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
) a;

SELECT *
FROM (
  SELECT 1 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION ALL
  SELECT 2 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
) a;

set hive.combine.equivalent.work.optimization = false;

explain
SELECT *
FROM (
  SELECT 1 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION ALL
  SELECT 2 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
) a;

SELECT *
FROM (
  SELECT 1 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION ALL
  SELECT 2 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
) a;
