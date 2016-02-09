set hive.fetch.task.conversion=none;

DROP TABLE IF EXISTS test;
CREATE TABLE test(ts TIMESTAMP) STORED AS ORC;
INSERT INTO TABLE test VALUES ('0001-01-01 00:00:00.000000000'), ('9999-12-31 23:59:59.999999999');

SET hive.vectorized.execution.enabled = false;
EXPLAIN
SELECT ts FROM test;

SELECT ts FROM test;

EXPLAIN
SELECT MIN(ts), MAX(ts), MAX(ts) - MIN(ts) FROM test;

SELECT MIN(ts), MAX(ts), MAX(ts) - MIN(ts) FROM test;

SET hive.vectorized.execution.enabled = true;
EXPLAIN
SELECT ts FROM test;

SELECT ts FROM test;

EXPLAIN
SELECT MIN(ts), MAX(ts), MAX(ts) - MIN(ts) FROM test;

SELECT MIN(ts), MAX(ts), MAX(ts) - MIN(ts) FROM test;