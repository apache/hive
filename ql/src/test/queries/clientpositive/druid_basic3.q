set hive.strict.checks.cartesian.product=false;
set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1_n4
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");

EXPLAIN
SELECT sum(added) + sum(delta) as a, language
FROM druid_table_1_n4
GROUP BY language
ORDER BY a DESC;

EXPLAIN
SELECT sum(delta), sum(added) + sum(delta) AS a, language
FROM druid_table_1_n4
GROUP BY language
ORDER BY a DESC;

EXPLAIN
SELECT language, sum(added) / sum(delta) AS a
FROM druid_table_1_n4
GROUP BY language
ORDER BY a DESC;
        
EXPLAIN
SELECT language, sum(added) * sum(delta) AS a
FROM druid_table_1_n4
GROUP BY language
ORDER BY a DESC;

EXPLAIN
SELECT language, sum(added) - sum(delta) AS a
FROM druid_table_1_n4
GROUP BY language
ORDER BY a DESC;
        
EXPLAIN
SELECT language, sum(added) + 100 AS a
FROM druid_table_1_n4
GROUP BY language
ORDER BY a DESC;

EXPLAIN
SELECT language, -1 * (a + b) AS c
FROM (
  SELECT (sum(added)-sum(delta)) / (count(*) * 3) AS a, sum(deleted) AS b, language
  FROM druid_table_1_n4
  GROUP BY language) subq
ORDER BY c DESC;

EXPLAIN
SELECT language, robot, sum(added) - sum(delta) AS a
FROM druid_table_1_n4
WHERE extract (week from `__time`) IN (10,11)
GROUP BY language, robot;

EXPLAIN
SELECT language, sum(delta) / count(*) AS a
FROM druid_table_1_n4
GROUP BY language
ORDER BY a DESC;

EXPLAIN
SELECT language, sum(added) / sum(delta) AS a,
       CASE WHEN sum(deleted)=0 THEN 1.0 ELSE sum(deleted) END AS b
FROM druid_table_1_n4
GROUP BY language
ORDER BY a DESC;

EXPLAIN
SELECT language, a, a - b as c
FROM (
  SELECT language, sum(added) + 100 AS a, sum(delta) AS b
  FROM druid_table_1_n4
  GROUP BY language) subq
ORDER BY a DESC;

EXPLAIN
SELECT language, robot, "A"
FROM (
  SELECT sum(added) - sum(delta) AS a, language, robot
  FROM druid_table_1_n4
  GROUP BY language, robot ) subq
ORDER BY "A"
LIMIT 5;

EXPLAIN
SELECT language, robot, "A"
FROM (
  SELECT language, sum(added) + sum(delta) AS a, robot
  FROM druid_table_1_n4
  GROUP BY language, robot) subq
ORDER BY robot, language
LIMIT 5;
