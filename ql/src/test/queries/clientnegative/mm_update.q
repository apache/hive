--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists mm_srcpart;
CREATE TABLE mm_srcpart (key STRING, value STRING) PARTITIONED BY (ds STRING, hr STRING) stored as ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
insert into mm_srcpart PARTITION (ds, hr) select * from srcpart;

select ds, hr, key, value from mm_srcpart where cast(key as integer) in(413,43) and hr='11' order by ds, hr, cast(key as integer);

update mm_srcpart set value = concat(value, 'updated') where cast(key as integer) in(413,43) and hr='11';

