--! qt:authorizer

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;



set user.name=user1;
create table auth_noupd(i int) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
delete from auth_noupd where i > 0;

set user.name=hive_admin_user;
set role admin;
