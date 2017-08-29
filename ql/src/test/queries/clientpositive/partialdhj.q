set hive.auto.convert.join=true;
set hive.strict.checks.cartesian.product=false;
set hive.merge.nway.joins=false;
set hive.optimize.shared.work=false;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.hashtable.max.entries=10;

-- ONE_TO_ONE ON BIG TABLE SIDE
EXPLAIN
SELECT *
FROM (
  SELECT a.value
  FROM src1 a
  JOIN src1 b
  ON (a.value = b.value)
  GROUP BY a.value
) a
JOIN src
ON (a.value = src.value);

SELECT *
FROM (
  SELECT a.value
  FROM src1 a
  JOIN src1 b
  ON (a.value = b.value)
  GROUP BY a.value
) a
JOIN src
ON (a.value = src.value);

-- ONE_TO_ONE ON SMALL TABLE SIDE
EXPLAIN
SELECT *
FROM src
JOIN (
  SELECT a.value
  FROM src1 a
  JOIN src1 b
  ON (a.value = b.value)
  GROUP BY a.value
) a
ON (src.value = a.value);

SELECT *
FROM src
JOIN (
  SELECT a.value
  FROM src1 a
  JOIN src1 b
  ON (a.value = b.value)
  GROUP BY a.value
) a
ON (src.value = a.value);
