set hive.strict.checks.cartesian.product=false;
set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");

DESCRIBE FORMATTED druid_table_1;

-- dimension
EXPLAIN EXTENDED
SELECT robot FROM druid_table_1;

-- metric
EXPLAIN EXTENDED
SELECT delta FROM druid_table_1;

EXPLAIN EXTENDED
SELECT robot
FROM druid_table_1
WHERE language = 'en';

EXPLAIN EXTENDED
SELECT DISTINCT robot
FROM druid_table_1
WHERE language = 'en';

-- TODO: currently nothing is pushed - ISNOTNULL
EXPLAIN EXTENDED
SELECT a.robot, b.language
FROM
(
  (SELECT robot, language
  FROM druid_table_1) a
  JOIN
  (SELECT language
  FROM druid_table_1) b
  ON a.language = b.language
);

EXPLAIN EXTENDED
SELECT a.robot, b.language
FROM
(
  (SELECT robot, language
  FROM druid_table_1
  WHERE language = 'en') a
  JOIN
  (SELECT language
  FROM druid_table_1) b
  ON a.language = b.language
);
