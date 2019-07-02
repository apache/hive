--! qt:dataset:src

-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE src_txn stored as orc TBLPROPERTIES ('transactional' = 'true')
AS SELECT * FROM src;

EXPLAIN
CREATE MATERIALIZED VIEW cluster_mv_1 PARTITIONED ON (partkey) CLUSTERED ON (key) AS
SELECT value, key, key + 100 as partkey FROM src_txn where key > 200 and key < 250;

CREATE MATERIALIZED VIEW cluster_mv_1 PARTITIONED ON (partkey) CLUSTERED ON (key) AS
SELECT value, key, key + 100 as partkey FROM src_txn where key > 200 and key < 250;

DESCRIBE FORMATTED cluster_mv_1;

dfs -ls ${system:test.warehouse.dir}/../localfs/warehouse/cluster_mv_1/;

EXPLAIN
SELECT * FROM cluster_mv_1 where key = 238;

SELECT * FROM cluster_mv_1 where key = 238;

CREATE MATERIALIZED VIEW cluster_mv_2 PARTITIONED ON (partkey) CLUSTERED ON (value) AS
SELECT key, value, key + 100 as partkey FROM src_txn where key > 200 and key < 250;

dfs -ls ${system:test.warehouse.dir}/../localfs/warehouse/cluster_mv_2/;

EXPLAIN
SELECT * FROM cluster_mv_2 where value = 'val_238';

SELECT * FROM cluster_mv_2 where value = 'val_238';

EXPLAIN
SELECT value FROM cluster_mv_2 where key = 238;

SELECT value FROM cluster_mv_2 where key = 238;

INSERT INTO src_txn VALUES (238, 'val_238_n');

EXPLAIN
ALTER MATERIALIZED VIEW cluster_mv_1 REBUILD;

ALTER MATERIALIZED VIEW cluster_mv_1 REBUILD;

SELECT * FROM cluster_mv_1 where key = 238;

SELECT * FROM cluster_mv_2 where key = 238;

CREATE TABLE src_txn_2 stored as orc TBLPROPERTIES ('transactional' = 'true')
AS SELECT * FROM src;

CREATE MATERIALIZED VIEW cluster_mv_3 PARTITIONED ON (partkey) CLUSTERED ON (key) AS
SELECT src_txn.key + 100 as partkey, src_txn.value, src_txn.key FROM src_txn, src_txn_2
WHERE src_txn.key = src_txn_2.key
  AND src_txn.key > 200 AND src_txn.key < 250;

dfs -ls ${system:test.warehouse.dir}/../localfs/warehouse/cluster_mv_3/;

INSERT INTO src_txn VALUES (238, 'val_238_n2');

EXPLAIN
ALTER MATERIALIZED VIEW cluster_mv_3 REBUILD;

ALTER MATERIALIZED VIEW cluster_mv_3 REBUILD;

SELECT * FROM cluster_mv_3 where key = 238;

CREATE MATERIALIZED VIEW cluster_mv_4 PARTITIONED ON (partkey) CLUSTERED ON (key,`tes"t`,`te*#"s"t`) AS
SELECT src_txn.key + 100 as partkey, value, key, key+1 as `tes"t`, key+2 as `te*#"s"t` FROM src_txn where key > 200 and key < 250;

DESCRIBE FORMATTED cluster_mv_4;

INSERT INTO src_txn VALUES (238, 'val_238_n2');

set hive.materializedview.rebuild.incremental=false;

EXPLAIN
ALTER MATERIALIZED VIEW cluster_mv_3 REBUILD;

ALTER MATERIALIZED VIEW cluster_mv_3 REBUILD;

SELECT * FROM cluster_mv_3 where key = 238;

EXPLAIN
ALTER MATERIALIZED VIEW cluster_mv_4 REBUILD;

ALTER MATERIALIZED VIEW cluster_mv_4 REBUILD;

SELECT * FROM cluster_mv_4 where key = 238;

DESCRIBE FORMATTED cluster_mv_4;
