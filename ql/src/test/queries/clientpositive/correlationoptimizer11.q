--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.auto.convert.join=false;
-- Tests in this file are used to make sure Correlation Optimizer
-- can correctly handle tables with partitions

CREATE TABLE part_table_n1(key string, value string) PARTITIONED BY (partitionId int);
INSERT OVERWRITE TABLE part_table_n1 PARTITION (partitionId=1)
  SELECT key, value FROM src ORDER BY key, value LIMIT 100;
INSERT OVERWRITE TABLE part_table_n1 PARTITION (partitionId=2)
  SELECT key, value FROM src1 ORDER BY key, value;

set hive.optimize.correlation=false;
-- In this case, we should not do shared scan on part_table_n1
-- because left and right tables of JOIN use different partitions
-- of part_table_n1. With Correlation Optimizer we will generate
-- 1 MR job.
EXPLAIN
SELECT x.key AS key, count(1) AS cnt
FROM part_table_n1 x JOIN part_table_n1 y ON (x.key = y.key)
WHERE x.partitionId = 1 AND
      y.partitionId = 2
GROUP BY x.key;

SELECT x.key AS key, count(1) AS cnt
FROM part_table_n1 x JOIN part_table_n1 y ON (x.key = y.key)
WHERE x.partitionId = 1 AND
      y.partitionId = 2
GROUP BY x.key;

set hive.optimize.correlation=true;
EXPLAIN
SELECT x.key AS key, count(1) AS cnt
FROM part_table_n1 x JOIN part_table_n1 y ON (x.key = y.key)
WHERE x.partitionId = 1 AND
      y.partitionId = 2
GROUP BY x.key;

SELECT x.key AS key, count(1) AS cnt
FROM part_table_n1 x JOIN part_table_n1 y ON (x.key = y.key)
WHERE x.partitionId = 1 AND
      y.partitionId = 2
GROUP BY x.key;

set hive.optimize.correlation=false;
-- In this case, we should do shared scan on part_table_n1
-- because left and right tables of JOIN use the same partition
-- of part_table_n1. With Correlation Optimizer we will generate
-- 1 MR job.
EXPLAIN
SELECT x.key AS key, count(1) AS cnt
FROM part_table_n1 x JOIN part_table_n1 y ON (x.key = y.key)
WHERE x.partitionId = 2 AND
      y.partitionId = 2
GROUP BY x.key;

SELECT x.key AS key, count(1) AS cnt
FROM part_table_n1 x JOIN part_table_n1 y ON (x.key = y.key)
WHERE x.partitionId = 2 AND
      y.partitionId = 2
GROUP BY x.key;

set hive.optimize.correlation=true;
EXPLAIN
SELECT x.key AS key, count(1) AS cnt
FROM part_table_n1 x JOIN part_table_n1 y ON (x.key = y.key)
WHERE x.partitionId = 2 AND
      y.partitionId = 2
GROUP BY x.key;

SELECT x.key AS key, count(1) AS cnt
FROM part_table_n1 x JOIN part_table_n1 y ON (x.key = y.key)
WHERE x.partitionId = 2 AND
      y.partitionId = 2
GROUP BY x.key;
