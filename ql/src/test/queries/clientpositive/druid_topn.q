set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1_n1
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");

DESCRIBE FORMATTED druid_table_1_n1;

-- GRANULARITY: ALL
EXPLAIN
SELECT robot, max(added) as m, sum(variation)
FROM druid_table_1_n1
GROUP BY robot
ORDER BY m DESC
LIMIT 100;

-- GRANULARITY: NONE
EXPLAIN
SELECT robot, `__time`, max(added), sum(variation) as s
FROM druid_table_1_n1
GROUP BY robot, `__time`
ORDER BY s DESC
LIMIT 100;

-- GRANULARITY: YEAR
EXPLAIN
SELECT robot, floor_year(`__time`), max(added), sum(variation) as s
FROM druid_table_1_n1
GROUP BY robot, floor_year(`__time`)
ORDER BY s DESC
LIMIT 10;

-- ASC: TRANSFORM INTO GROUP BY
EXPLAIN
SELECT robot, floor_month(`__time`), max(added), sum(variation) as s
FROM druid_table_1_n1
GROUP BY robot, floor_month(`__time`)
ORDER BY s
LIMIT 10;

-- MULTIPLE ORDER: TRANSFORM INTO GROUP BY
EXPLAIN
SELECT robot, floor_month(`__time`), max(added) as m, sum(variation) as s
FROM druid_table_1_n1
GROUP BY robot, namespace, floor_month(`__time`)
ORDER BY s DESC, m DESC
LIMIT 10;

-- MULTIPLE ORDER MIXED: TRANSFORM INTO GROUP BY
EXPLAIN
SELECT robot, floor_month(`__time`), max(added) as m, sum(variation) as s
FROM druid_table_1_n1
GROUP BY robot, namespace, floor_month(`__time`)
ORDER BY robot ASC, m DESC
LIMIT 10;

-- WITH FILTER ON DIMENSION: TRANSFORM INTO GROUP BY
EXPLAIN
SELECT robot, floor_year(`__time`), max(added), sum(variation) as s
FROM druid_table_1_n1
WHERE robot='1'
GROUP BY robot, floor_year(`__time`)
ORDER BY s
LIMIT 10;

-- WITH FILTER ON TIME
EXPLAIN
SELECT robot, floor_hour(`__time`), max(added) as m, sum(variation)
FROM druid_table_1_n1
WHERE floor_hour(`__time`)
    BETWEEN CAST('2010-01-01 00:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)
        AND CAST('2014-01-01 00:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)
GROUP BY robot, floor_hour(`__time`)
ORDER BY m
LIMIT 100;
