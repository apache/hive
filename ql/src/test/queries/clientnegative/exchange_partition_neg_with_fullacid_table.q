--! qt:dataset:src
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a int) PARTITIONED BY (d1 int) STORED AS ORC TBLPROPERTIES ("transactional"="true");
CREATE TABLE t2 (a int) PARTITIONED BY (d1 int) STORED AS ORC TBLPROPERTIES ("transactional"="true");
set hive.mapred.mode=nonstrict;

INSERT INTO TABLE t1 PARTITION (d1 = 1) SELECT key FROM src where key = 100 limit 1;
SELECT * FROM t1;

ALTER TABLE t2 EXCHANGE PARTITION (d1 = 1) WITH TABLE t1;
