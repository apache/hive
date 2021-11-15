--! qt:authorizer

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;



-- check update without update priv
create table auth_nodel(i int) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

set user.name=user1;
delete from auth_nodel where i > 0;

