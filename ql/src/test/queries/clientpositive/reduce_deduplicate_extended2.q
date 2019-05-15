--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;
set hive.convert.join.bucket.mapjoin.tez=false;
set hive.optimize.dynamic.partition.hashjoin=false;
set hive.limit.pushdown.memory.usage=0.3f;
set hive.optimize.reducededuplication.min.reducer=1;

-- JOIN + GBY
EXPLAIN
SELECT f.key, g.value
FROM src f
JOIN src g ON (f.key = g.key AND f.value = g.value)
GROUP BY g.value, f.key;

-- JOIN + GBY + OBY
EXPLAIN
SELECT g.key, f.value
FROM src f
JOIN src g ON (f.key = g.key AND f.value = g.value)
GROUP BY g.key, f.value
ORDER BY f.value, g.key;

-- GBY + JOIN + GBY
EXPLAIN
SELECT f.key, g.value
FROM src f
JOIN (
  SELECT key, value
  FROM src
  GROUP BY key, value) g
ON (f.key = g.key AND f.value = g.value)
GROUP BY g.value, f.key;

-- 2GBY + JOIN + GBY
EXPLAIN
SELECT f.key, g.value
FROM (
  SELECT key, value
  FROM src
  GROUP BY value, key) f
JOIN (
  SELECT key, value
  FROM src
  GROUP BY key, value) g
ON (f.key = g.key AND f.value = g.value)
GROUP BY g.value, f.key;

-- 2GBY + JOIN + GBY + OBY
EXPLAIN
SELECT f.key, g.value
FROM (
  SELECT value
  FROM src
  GROUP BY value) g
JOIN (
  SELECT key
  FROM src
  GROUP BY key) f
GROUP BY g.value, f.key
ORDER BY f.key desc, g.value;

-- 2(2GBY + JOIN + GBY + OBY) + UNION
EXPLAIN
SELECT x.key, x.value
FROM (
  SELECT f.key, g.value
  FROM (
    SELECT key, value
    FROM src
    GROUP BY key, value) f
  JOIN (
    SELECT key, value
    FROM src
    GROUP BY value, key) g
  ON (f.key = g.key AND f.value = g.value)
  GROUP BY g.value, f.key
UNION ALL
  SELECT f.key, g.value
  FROM (
    SELECT key, value
    FROM src
    GROUP BY value, key) f
  JOIN (
    SELECT key, value
    FROM src
    GROUP BY key, value) g
  ON (f.key = g.key AND f.value = g.value)
  GROUP BY f.key, g.value
) x
ORDER BY x.value desc, x.key desc;
