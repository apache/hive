--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists transactional_insert_only_table;

create transactional table transactional_insert_only_table(key string, value string) PARTITIONED BY(ds string) CLUSTERED BY(key) INTO 2 BUCKETS;

desc formatted transactional_insert_only_table;

insert into table transactional_insert_only_table partition(ds)  select key,value,ds from srcpart;