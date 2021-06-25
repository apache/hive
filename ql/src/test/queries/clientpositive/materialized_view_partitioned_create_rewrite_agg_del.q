set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.optimize.sort.dynamic.partition=true;
--set hive.vectorized.execution.enabled=false;

CREATE TABLE t1(a int, b int,c int) STORED AS ORC TBLPROPERTIES ('transactional' = 'true');

INSERT INTO t1(a, b, c) VALUES
(1, 1, 1),
(1, 1, 4), -- del
(2, 1, 2),
(1, 2, 10),
(2, 2, 11),
(1, 3, 100); -- del

CREATE MATERIALIZED VIEW mat1 PARTITIONED ON (a) STORED AS ORC TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only") AS
SELECT a, b, sum(c) sumc, count(*) countstar FROM t1 GROUP BY b, a;

DELETE FROM t1 WHERE c = 4 OR c = 100;

SELECT a, b, sum(sumc), sum(countstar) counttotal FROM (
    SELECT a, b, sumc, countstar FROM mat1
    LEFT SEMI JOIN (SELECT a, b, sum(c), count(*) FROM t1('acid.fetch.deleted.rows'='true') WHERE ROW__ID.writeId > 1 GROUP BY b, a) q ON (mat1.a <=> q.a)
    UNION ALL
    SELECT a, b, sum(CASE WHEN t1.ROW__IS__DELETED THEN -c ELSE c END) sumc, sum(CASE WHEN t1.ROW__IS__DELETED THEN -1 ELSE 1 END) countstar FROM t1('acid.fetch.deleted.rows'='true') WHERE ROW__ID.writeId > 1 GROUP BY b, a
) sub
GROUP BY a, b
HAVING counttotal > 0
ORDER BY a, b;

EXPLAIN CBO
ALTER MATERIALIZED VIEW mat1 REBUILD;
EXPLAIN
ALTER MATERIALIZED VIEW mat1 REBUILD;
ALTER MATERIALIZED VIEW mat1 REBUILD;

SELECT a, b, sumc, countstar FROM mat1 ORDER BY a;

DROP MATERIALIZED VIEW mat1;

SELECT a, b, sum(c) sumc, count(*) countstar FROM t1 GROUP BY b, a;
