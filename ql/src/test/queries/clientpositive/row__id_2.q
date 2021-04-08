-- tid is flaky when compute column stats
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.support.concurrency=true;

create table t1(a int, b float) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(a, b) values (1, 1.1);
insert into t1(a, b) values (2, 2.2);

SELECT t1.ROW__ID, A, b FROM t1 WHERE t1.ROW__ID.writeid > 1;
