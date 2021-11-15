--! qt:authorizer

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;



set user.name=user1;
create table auth_noupd_n0(i int, j int) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
update auth_noupd_n0 set j = 0 where i > 0;

set user.name=hive_admin_user;
set role admin;
