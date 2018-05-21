set hive.strict.checks.cartesian.product=false;
set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1_n2
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");

DESCRIBE FORMATTED druid_table_1_n2;

-- dimension
EXPLAIN EXTENDED
SELECT robot FROM druid_table_1_n2;

-- metric
EXPLAIN EXTENDED
SELECT delta FROM druid_table_1_n2;

EXPLAIN EXTENDED
SELECT robot
FROM druid_table_1_n2
WHERE language = 'en';

EXPLAIN EXTENDED
SELECT DISTINCT robot
FROM druid_table_1_n2
WHERE language = 'en';

-- TODO: currently nothing is pushed - ISNOTNULL
EXPLAIN EXTENDED
SELECT a.robot, b.language
FROM
(
  (SELECT robot, language
  FROM druid_table_1_n2) a
  JOIN
  (SELECT language
  FROM druid_table_1_n2) b
  ON a.language = b.language
);

EXPLAIN EXTENDED
SELECT a.robot, b.language
FROM
(
  (SELECT robot, language
  FROM druid_table_1_n2
  WHERE language = 'en') a
  JOIN
  (SELECT language
  FROM druid_table_1_n2) b
  ON a.language = b.language
);

EXPLAIN EXTENDED
SELECT robot, floor_day(`__time`), max(added) as m, sum(delta) as s
FROM druid_table_1_n2
GROUP BY robot, language, floor_day(`__time`)
ORDER BY CAST(robot AS INTEGER) ASC, m DESC
LIMIT 10;

EXPLAIN
SELECT substring(namespace, CAST(deleted AS INT), 4)
FROM druid_table_1_n2;

EXPLAIN
SELECT robot, floor_day(`__time`)
FROM druid_table_1_n2
WHERE floor_day(`__time`) BETWEEN '1999-11-01 00:00:00' AND '1999-11-10 00:00:00'
GROUP BY robot, floor_day(`__time`)
ORDER BY robot
LIMIT 10;

EXPLAIN
SELECT robot, `__time`
FROM druid_table_1_n2
WHERE floor_day(`__time`) BETWEEN '1999-11-01 00:00:00' AND '1999-11-10 00:00:00'
GROUP BY robot, `__time`
ORDER BY robot
LIMIT 10;

EXPLAIN
SELECT robot, floor_day(`__time`)
FROM druid_table_1_n2
WHERE `__time` BETWEEN '1999-11-01 00:00:00' AND '1999-11-10 00:00:00'
GROUP BY robot, floor_day(`__time`)
ORDER BY robot
LIMIT 10;

-- No CBO test: it should work
set hive.cbo.enable=false;
EXPLAIN EXTENDED
SELECT robot, floor_day(`__time`), max(added) as m, sum(delta) as s
FROM druid_table_1_n2
GROUP BY robot, language, floor_day(`__time`)
ORDER BY CAST(robot AS INTEGER) ASC, m DESC
LIMIT 10;
