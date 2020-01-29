--! qt:dataset:src

set hive.cbo.enable=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

EXPLAIN
CREATE TABLE x STORED AS ORC TBLPROPERTIES('transactional'='true') AS
SELECT * FROM SRC x CLUSTER BY x.key;
CREATE TABLE x STORED AS ORC TBLPROPERTIES('transactional'='true') AS
SELECT * FROM SRC x CLUSTER BY x.key;
DROP TABLE x;
