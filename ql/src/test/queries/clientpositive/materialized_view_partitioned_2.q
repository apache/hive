--! qt:dataset:src

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE src_txn_2 stored as orc TBLPROPERTIES ('transactional' = 'true')
AS SELECT * FROM src where key > 200 and key < 300;

CREATE MATERIALIZED VIEW partition_mv_1 PARTITIONED ON (key) AS
SELECT value, key FROM src_txn_2;

CREATE MATERIALIZED VIEW partition_mv_2 PARTITIONED ON (value) AS
SELECT key, value FROM src_txn_2;

CREATE MATERIALIZED VIEW partition_mv_3 PARTITIONED ON (key) AS
SELECT value, key FROM src_txn_2 where key > 220 and key < 230;

CREATE MATERIALIZED VIEW partition_mv_4 PARTITIONED ON (key) AS
SELECT value, key FROM src_txn_2 where key > 222 and key < 228;

-- SHOULD CHOOSE partition_mv_1 SINCE ONLY partition_mv_1 AND
-- partition_mv_2 ARE VALID BUT PRUNING KICKS IN FOR THE FORMER
EXPLAIN
SELECT * FROM src_txn_2 where key > 210 and key < 230;

SELECT * FROM src_txn_2 where key > 210 and key < 230;

-- SHOULD CHOOSE partition_mv_2 SINCE ONLY partition_mv_2 AND
-- partition_mv_1 ARE VALID BUT PRUNING KICKS IN FOR THE FORMER
EXPLAIN
SELECT * FROM src_txn_2 where value > 'val_220' and value < 'val_230';

SELECT * FROM src_txn_2 where value > 'val_220' and value < 'val_230';

-- SHOULD CHOOSE partition_mv_4 SINCE IT IS THE MOST EFFICIENT
-- READING ONLY ONE PARTITION
EXPLAIN
SELECT * FROM src_txn_2 where key > 224 and key < 226;

SELECT * FROM src_txn_2 where key > 223 and key < 225;

DROP MATERIALIZED VIEW partition_mv_1;
DROP MATERIALIZED VIEW partition_mv_2;
DROP MATERIALIZED VIEW partition_mv_3;
DROP MATERIALIZED VIEW partition_mv_4;
DROP TABLE src_txn_2;
