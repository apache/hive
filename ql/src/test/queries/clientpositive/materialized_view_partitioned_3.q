--! qt:dataset:src

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE src_txn stored as orc TBLPROPERTIES ('transactional' = 'true')
AS SELECT * FROM src;

EXPLAIN
CREATE MATERIALIZED VIEW partition_mv_sdp PARTITIONED ON (key) AS
SELECT value, key FROM src_txn where key > 200 and key < 250;
