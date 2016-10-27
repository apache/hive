set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table concat_mm (id int) stored as orc tblproperties("transactional"="true", "transactional_properties"="insert_only");

insert into table concat_mm select key from src limit 10;

alter table concat_mm concatenate;
