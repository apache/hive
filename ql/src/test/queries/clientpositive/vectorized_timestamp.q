set hive.fetch.task.conversion=none;
set hive.explain.user=false;
set hive.vectorized.execution.reduce.enabled=true;

DROP TABLE IF EXISTS test_n2;
CREATE TABLE test_n2(ts TIMESTAMP) STORED AS ORC;
INSERT INTO TABLE test_n2 VALUES ('0001-01-01 00:00:00.000000000'), ('9999-12-31 23:59:59.999999999');

SET hive.vectorized.execution.enabled = false;
EXPLAIN VECTORIZATION DETAIL
SELECT ts FROM test_n2;

SELECT ts FROM test_n2;

SELECT MIN(ts), MAX(ts), MAX(ts) - MIN(ts) FROM test_n2;

SELECT ts FROM test_n2 WHERE ts IN (timestamp '0001-01-01 00:00:00.000000000', timestamp '0002-02-02 00:00:00.000000000');

SET hive.vectorized.execution.enabled = true;

SELECT ts FROM test_n2;

EXPLAIN VECTORIZATION DETAIL
SELECT MIN(ts), MAX(ts), MAX(ts) - MIN(ts) FROM test_n2;

SELECT MIN(ts), MAX(ts), MAX(ts) - MIN(ts) FROM test_n2;

EXPLAIN VECTORIZATION DETAIL
SELECT ts FROM test_n2 WHERE ts IN (timestamp '0001-01-01 00:00:00.000000000', timestamp '0002-02-02 00:00:00.000000000');

SELECT ts FROM test_n2 WHERE ts IN (timestamp '0001-01-01 00:00:00.000000000', timestamp '0002-02-02 00:00:00.000000000');

EXPLAIN VECTORIZATION DETAIL
SELECT AVG(ts), CAST(AVG(ts) AS TIMESTAMP) FROM test_n2;

SELECT AVG(ts), CAST(AVG(ts) AS TIMESTAMP) FROM test_n2;

EXPLAIN VECTORIZATION DETAIL
SELECT variance(ts), var_pop(ts), var_samp(ts), std(ts), stddev(ts), stddev_pop(ts), stddev_samp(ts) FROM test_n2;

SELECT variance(ts), var_pop(ts), var_samp(ts), std(ts), stddev(ts), stddev_pop(ts), stddev_samp(ts) FROM test_n2;
