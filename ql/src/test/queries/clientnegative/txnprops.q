--! qt:dataset:src

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table test_props;
create table test_props TBLPROPERTIES ("transactional_properties"="insert_only") as select * from src limit 1;
