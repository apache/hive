SET hive.vectorized.execution.enabled=false;
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.materializedview.rewriting.sql=false;
set hive.cli.print.header=true;

CREATE TABLE t1 (a int, b varchar(256), c decimal(10,2), d int) STORED AS orc TBLPROPERTIES ('transactional'='true');

INSERT INTO t1 VALUES
 (NULL, 'null_value', 100.77, 7),
 (1, 'calvin', 978.76, 3),
 (1, 'charlie', 9.8, 1);

CREATE MATERIALIZED VIEW mat1 TBLPROPERTIES ('transactional'='true') AS
  SELECT a, b, sum(d)
  FROM t1
  WHERE c > 10.0
  GROUP BY a, b;

INSERT INTO t1 VALUES
 (NULL, 'null_value', 100.88, 8),
 (NULL, NULL, 20.2, 200),
 (9, 'bob', 40.2, 400),
 (1, 'charlie', 15.8, 1);

explain cbo
ALTER MATERIALIZED VIEW mat1 REBUILD;
explain
ALTER MATERIALIZED VIEW mat1 REBUILD;
ALTER MATERIALIZED VIEW mat1 REBUILD;

EXPLAIN CBO
SELECT a, b, sum(d)
FROM t1
WHERE c > 10.0
GROUP BY a, b
ORDER BY a, b;

SELECT a, b, sum(d)
FROM t1
WHERE c > 10.0
GROUP BY a, b
ORDER BY a, b;

SELECT * FROM mat1
ORDER BY a, b;

DROP MATERIALIZED VIEW mat1;

EXPLAIN CBO
SELECT a, b, sum(d)
FROM t1
WHERE c > 10.0
GROUP BY a, b
ORDER BY a, b;

SELECT a, b, sum(d)
FROM t1
WHERE c > 10.0
GROUP BY a, b
ORDER BY a, b;
