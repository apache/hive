--! qt:authorizer

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;



-- check update without update priv
create table auth_noupd(i int, j int) clustered by (j) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

set user.name=user1;
update auth_noupd set i = 0 where i > 0;

