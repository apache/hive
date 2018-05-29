SET hive.vectorized.execution.enabled=true;
SET hive.llap.io.enabled=true;
SET hive.map.aggr=false;
SET hive.exec.post.hooks=;

CREATE TABLE test_n7(f1 int, f2 int, f3 int) stored as orc;
INSERT INTO TABLE test_n7 VALUES (1,1,1), (2,2,2), (3,3,3), (4,4,4);

ALTER TABLE test_n7 CHANGE f1 f1 bigint;
ALTER TABLE test_n7 CHANGE f2 f2 bigint;
ALTER TABLE test_n7 CHANGE f3 f3 bigint;

-- llap counters with data and meta cache
SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
SELECT count(f1) FROM test_n7 GROUP BY f1;
SELECT count(f1) FROM test_n7 GROUP BY f1;

SET hive.exec.post.hooks=;
CREATE TABLE test_bigint(f1 bigint, f2 bigint, f3 bigint) stored as orc;
INSERT OVERWRITE TABLE test_bigint select * from test_n7;
ALTER TABLE test_bigint CHANGE f1 f1 double;
ALTER TABLE test_bigint CHANGE f2 f2 double;
ALTER TABLE test_bigint CHANGE f3 f3 double;

-- llap counters with meta cache alone
SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
select count(f1) from test_bigint group by f1;
select count(f1) from test_bigint group by f1;


-- Check with ACID table
SET hive.exec.post.hooks=;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.support.concurrency=true;
CREATE TABLE test_acid_n0 (f1 int, f2 int, val string) clustered by (val) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
INSERT INTO TABLE test_acid_n0 VALUES (1,1,'b1'), (2,2,'b2'), (3,3,'b3'), (4,4,'b4');

-- should not have llap counters
SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
SELECT count(f1) FROM test_acid_n0 GROUP BY f1;
