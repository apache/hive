set hive.mapred.mode=nonstrict;
-- SORT_BEFORE_DIFF
EXPLAIN
SELECT count(1) FROM (
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL

  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL

  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL

  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL

  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src) src;


SELECT count(1) FROM (
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL

  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL

  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL

  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL

  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src UNION ALL
  SELECT key, value FROM src) src;
