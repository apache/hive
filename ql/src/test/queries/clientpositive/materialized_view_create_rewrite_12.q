-- Test rebuild of materialized view without aggregate when source tables have delete operations since last rebuild.
-- Incremental rebuild is not available.

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table t1 (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1 values
(3, 3),
(2, 1),
(2, 2),
(1, 2),
(1, 1);

CREATE MATERIALIZED VIEW mat1
  TBLPROPERTIES ('transactional'='true') AS
SELECT a
FROM t1
WHERE b < 10;

delete from t1 where b = 2;

explain
alter materialized view mat1 rebuild;

alter materialized view mat1 rebuild;

explain cbo
SELECT a
FROM t1
WHERE b < 10;

SELECT a
FROM t1
WHERE b < 10;
