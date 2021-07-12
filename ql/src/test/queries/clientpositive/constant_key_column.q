--! qt:dataset:src
SET hive.remove.orderby.in.subquery=false;

-- SORT_QUERY_RESULTS

EXPLAIN
SELECT constant_col, key, max(value)
FROM
(
  SELECT 'constant' as constant_col, key, value
  FROM src
  DISTRIBUTE BY constant_col, key
  SORT BY constant_col, key, value
) a
GROUP BY constant_col, key
ORDER BY constant_col, key
LIMIT 10;

SELECT constant_col, key, max(value)
FROM
(
  SELECT 'constant' as constant_col, key, value
  FROM src
  DISTRIBUTE BY constant_col, key
  SORT BY constant_col, key, value
) a
GROUP BY constant_col, key
ORDER BY constant_col, key
LIMIT 10;
