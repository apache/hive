--! qt:dataset:src

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE src_txn stored as orc TBLPROPERTIES ('transactional' = 'true')
AS SELECT * FROM src;

EXPLAIN
CREATE MATERIALIZED VIEW distribute_mv_1 DISTRIBUTED ON (key) SORTED ON (value) STORED AS TEXTFILE AS
SELECT value, key FROM src_txn where key > 200 and key < 250;

CREATE MATERIALIZED VIEW distribute_mv_1 DISTRIBUTED ON (key) SORTED ON (value) STORED AS TEXTFILE AS
SELECT value, key FROM src_txn where key > 200 and key < 250;

DESCRIBE FORMATTED distribute_mv_1;

dfs -ls ${system:test.warehouse.dir}/../localfs/warehouse/distribute_mv_1/;
dfs -cat ${system:test.warehouse.dir}/../localfs/warehouse/distribute_mv_1/*;

EXPLAIN
SELECT * FROM distribute_mv_1 where key = 238;

SELECT * FROM distribute_mv_1 where key = 238;

EXPLAIN
CREATE MATERIALIZED VIEW distribute_mv_2 DISTRIBUTED ON (value) SORTED ON (value, key) STORED AS TEXTFILE AS
SELECT key, value FROM src_txn where key > 200 and key < 250;

CREATE MATERIALIZED VIEW distribute_mv_2 DISTRIBUTED ON (value) SORTED ON (value, key) STORED AS TEXTFILE AS
SELECT key, value FROM src_txn where key > 200 and key < 250;

DESCRIBE FORMATTED distribute_mv_2;

dfs -ls ${system:test.warehouse.dir}/../localfs/warehouse/distribute_mv_2/;
dfs -cat ${system:test.warehouse.dir}/../localfs/warehouse/distribute_mv_2/*;

EXPLAIN
SELECT * FROM distribute_mv_2 where value = 'val_238';

SELECT * FROM distribute_mv_2 where value = 'val_238';

EXPLAIN
SELECT value FROM distribute_mv_2 where key = 238;

SELECT value FROM distribute_mv_2 where key = 238;

INSERT INTO src_txn VALUES (238, 'val_238_n');

EXPLAIN
ALTER MATERIALIZED VIEW distribute_mv_1 REBUILD;

ALTER MATERIALIZED VIEW distribute_mv_1 REBUILD;

SELECT * FROM distribute_mv_1 where key = 238;

SELECT * FROM distribute_mv_2 where key = 238;

CREATE TABLE src_txn_2 stored as orc TBLPROPERTIES ('transactional' = 'true')
AS SELECT * FROM src;

EXPLAIN
CREATE MATERIALIZED VIEW distribute_mv_3 DISTRIBUTED ON (key) SORTED ON (value, key) STORED AS TEXTFILE AS
SELECT src_txn.value, src_txn.key FROM src_txn, src_txn_2
WHERE src_txn.key = src_txn_2.key
  AND src_txn.key > 200 AND src_txn.key < 250;

CREATE MATERIALIZED VIEW distribute_mv_3 DISTRIBUTED ON (key) SORTED ON (value, key) STORED AS TEXTFILE AS
SELECT src_txn.value, src_txn.key FROM src_txn, src_txn_2
WHERE src_txn.key = src_txn_2.key
  AND src_txn.key > 200 AND src_txn.key < 250;

DESCRIBE FORMATTED distribute_mv_3;

dfs -ls ${system:test.warehouse.dir}/../localfs/warehouse/distribute_mv_3/;
dfs -cat ${system:test.warehouse.dir}/../localfs/warehouse/distribute_mv_3/*;

INSERT INTO src_txn VALUES (238, 'val_238_n2');

EXPLAIN
ALTER MATERIALIZED VIEW distribute_mv_3 REBUILD;

ALTER MATERIALIZED VIEW distribute_mv_3 REBUILD;

SELECT * FROM distribute_mv_3 where key = 238;
