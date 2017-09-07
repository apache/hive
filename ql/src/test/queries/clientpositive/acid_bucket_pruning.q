set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.optimize.index.filter=true;
set hive.tez.bucket.pruning=true;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- Bucket pruning only works for ACID when split-update (U=D+I) has been enabled for the table.
-- For e.g., this can be done by setting 'transactional_properties' = 'default'.
-- This also means that bucket pruning will not work for ACID tables with legacy behaviour.

CREATE TABLE acidTblDefault(a INT) CLUSTERED BY(a) INTO 16 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default');
INSERT INTO TABLE acidTblDefault SELECT cint FROM alltypesorc WHERE cint IS NOT NULL ORDER BY cint;
INSERT INTO TABLE acidTblDefault VALUES (1);

-- Exactly one of the buckets should be selected out of the 16 buckets
-- by the following selection query.
EXPLAIN EXTENDED
SELECT * FROM acidTblDefault WHERE a = 1;

select count(*) from acidTblDefault WHERE a = 1;

set hive.tez.bucket.pruning=false;

select count(*) from acidTblDefault WHERE a = 1;
