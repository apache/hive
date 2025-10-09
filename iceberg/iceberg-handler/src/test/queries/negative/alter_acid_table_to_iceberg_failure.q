set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table tbl_orc;
create table tbl_orc (a int, b string) stored as orc tblproperties('transactional'='true');
alter table tbl_orc convert to iceberg;