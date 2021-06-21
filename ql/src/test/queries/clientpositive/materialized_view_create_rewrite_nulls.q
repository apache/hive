SET hive.vectorized.execution.enabled=false;
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.materializedview.rewriting.sql=false;
set hive.cli.print.header=true;

CREATE TABLE t1 (a int, b varchar(256), c decimal(10,2), d int) STORED AS orc TBLPROPERTIES ('transactional'='true');

INSERT INTO t1 VALUES
 (NULL, 'row with NULL GBY key', 100.77, 7),
 (NULL, 'row with NULL GBY key', 100.77, 8),
 (NULL, 'row with NULL GBY key will be inserted', 100.77, 7),
 (1, 'row with NULL aggregated value', 100.77, NULL),
 (1, 'row with NULL aggregated value will be inserted', 100.77, NULL),
 (2, 'average row', 100, 10),
 (2, 'average row', 100, 11),
 (3, 'average row will be inserted', 100, 20),
 (4, 'row with NULL aggregated value + non null value will be inserted', 100.77, NULL),
 (5, 'average row + null value will be inserted', 100, 11),
 (NULL, NULL, 200, 100);

CREATE MATERIALIZED VIEW mat1 TBLPROPERTIES ('transactional'='true') AS
  SELECT a, b, sum(d), min(d), max(d)
  FROM t1
  WHERE c > 10.0
  GROUP BY a, b;

INSERT INTO t1 VALUES
 (NULL, 'new row with NULL GBY key', 100.77, 35),
 (NULL, 'new row with NULL GBY key', 100.77, 36),
 (NULL, 'row with NULL GBY key will be inserted', 100.77, 7),
 (1, 'new row with NULL aggregated value', 100.77, NULL),
 (1, 'row with NULL aggregated value will be inserted', 100.77, NULL),
 (2, 'new average row', 100, 50),
 (2, 'new average row', 100, 51),
 (3, 'average row will be inserted', 100, 20),
 (4, 'row with NULL aggregated value + non null value will be inserted', 100.77, 100),
 (5, 'average row + null value will be inserted', 100, NULL),
 (NULL, NULL, 200, 100);

explain cbo
ALTER MATERIALIZED VIEW mat1 REBUILD;
explain
ALTER MATERIALIZED VIEW mat1 REBUILD;
ALTER MATERIALIZED VIEW mat1 REBUILD;

EXPLAIN CBO
SELECT a, b, sum(d), min(d), max(d)
FROM t1
WHERE c > 10.0
GROUP BY a, b
ORDER BY a, b;

SELECT a, b, sum(d), min(d), max(d)
FROM t1
WHERE c > 10.0
GROUP BY a, b
ORDER BY a, b;

SELECT * FROM mat1
ORDER BY a, b;

DROP MATERIALIZED VIEW mat1;

EXPLAIN CBO
SELECT a, b, sum(d), min(d), max(d)
FROM t1
WHERE c > 10.0
GROUP BY a, b
ORDER BY a, b;

SELECT a, b, sum(d), min(d), max(d)
FROM t1
WHERE c > 10.0
GROUP BY a, b
ORDER BY a, b;
