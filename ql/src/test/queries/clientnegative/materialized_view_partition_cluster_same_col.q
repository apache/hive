--! qt:dataset:src

-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE src_txn stored as orc TBLPROPERTIES ('transactional' = 'true')
AS SELECT * FROM src;

CREATE MATERIALIZED VIEW cluster_mv_1 PARTITIONED ON (key) CLUSTERED ON (key) AS
SELECT value, key, key + 100 as partkey FROM src_txn where key > 200 and key < 250;
