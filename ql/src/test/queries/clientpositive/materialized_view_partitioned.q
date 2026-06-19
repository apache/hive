--! qt:dataset:src

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE src_txn stored as orc TBLPROPERTIES ('transactional' = 'true')
AS SELECT * FROM src;

EXPLAIN
CREATE MATERIALIZED VIEW partition_mv_1 PARTITIONED ON (key) AS
SELECT value, key FROM src_txn where key > 200 and key < 250;

CREATE MATERIALIZED VIEW partition_mv_1 PARTITIONED ON (key) AS
SELECT value, key FROM src_txn where key > 200 and key < 250;

DESCRIBE FORMATTED partition_mv_1;

EXPLAIN
SELECT * FROM partition_mv_1 where key = 238;

SELECT * FROM partition_mv_1 where key = 238;

CREATE MATERIALIZED VIEW partition_mv_2 PARTITIONED ON (value) AS
SELECT key, value FROM src_txn where key > 200 and key < 250;

EXPLAIN
SELECT * FROM partition_mv_2 where value = 'val_238';

SELECT * FROM partition_mv_2 where value = 'val_238';

EXPLAIN
SELECT value FROM partition_mv_2 where key = 238;

SELECT value FROM partition_mv_2 where key = 238;

INSERT INTO src_txn VALUES (238, 'val_238_n');

EXPLAIN
ALTER MATERIALIZED VIEW partition_mv_1 REBUILD;

ALTER MATERIALIZED VIEW partition_mv_1 REBUILD;

SELECT * FROM partition_mv_1 where key = 238;

SELECT * FROM partition_mv_2 where key = 238;

CREATE TABLE src_txn_2 stored as orc TBLPROPERTIES ('transactional' = 'true')
AS SELECT * FROM src;

CREATE MATERIALIZED VIEW partition_mv_3 PARTITIONED ON (key) AS
SELECT src_txn.value, src_txn.key FROM src_txn, src_txn_2
WHERE src_txn.key = src_txn_2.key
  AND src_txn.key > 200 AND src_txn.key < 250;

INSERT INTO src_txn VALUES (238, 'val_238_n2');

EXPLAIN
ALTER MATERIALIZED VIEW partition_mv_3 REBUILD;

ALTER MATERIALIZED VIEW partition_mv_3 REBUILD;

SELECT * FROM partition_mv_3 where key = 238;

DROP MATERIALIZED VIEW partition_mv_1;
DROP MATERIALIZED VIEW partition_mv_2;
DROP MATERIALIZED VIEW partition_mv_3;
