set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE t1(a int, b int,c int) STORED AS ORC TBLPROPERTIES ('transactional' = 'true');

INSERT INTO t1(a, b, c) VALUES
(1, 1, 1),
(1, 1, 4),
(2, 1, 2),
(1, 2, 10),
(2, 2, 11),
(1, 3, 100),
(null, 4, 200);

CREATE MATERIALIZED VIEW mat1 PARTITIONED ON (a) STORED AS ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') AS
SELECT a, b, sum(c) sumc FROM t1 GROUP BY b, a;

INSERT INTO t1(a, b, c) VALUES
(1, 1, 3),
(1, 3, 110),
(null, 4, 20);

SELECT b, sum(sumc), a FROM (
    SELECT b, sumc, a FROM mat1
    LEFT SEMI JOIN (SELECT b, sum(c), a FROM t1 WHERE ROW__ID.writeId > 1 GROUP BY b, a) q ON (mat1.a <=> q.a)
    UNION ALL
    SELECT b, sum(c) sumc, a FROM t1 WHERE ROW__ID.writeId > 1 GROUP BY b, a
) sub
GROUP BY b, a
ORDER BY a, b;

EXPLAIN CBO
ALTER MATERIALIZED VIEW mat1 REBUILD;
EXPLAIN
ALTER MATERIALIZED VIEW mat1 REBUILD;
ALTER MATERIALIZED VIEW mat1 REBUILD;

SELECT b, sumc, a FROM mat1
order by a, b;

DROP MATERIALIZED VIEW mat1;

SELECT b, sum(c), a sumc FROM t1 GROUP BY b, a
order by a, b;
