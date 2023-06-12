set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create external table acid_external (a int, b varchar(128)) clustered by (b) into 2 buckets stored as orc;

alter table acid_external convert to Acid;

drop table acid_external;