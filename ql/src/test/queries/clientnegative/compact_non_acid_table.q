set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;


create table not_an_acid_table (a int, b varchar(128));

alter table not_an_acid_table compact 'major';

drop table not_an_acid_table;