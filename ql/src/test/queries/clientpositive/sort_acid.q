-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table acidtlb(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table othertlb(c int, d int) stored as orc TBLPROPERTIES ('transactional'='true');

