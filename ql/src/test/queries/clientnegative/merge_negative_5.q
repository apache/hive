set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled=false;
set hive.explain.user=false;
set hive.merge.cardinality.check=true;

drop table if exists srcpart_acid;
CREATE TABLE srcpart_acid (key STRING, value STRING) PARTITIONED BY (ds STRING, hr STRING) stored as ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default');
insert into srcpart_acid PARTITION (ds, hr) select * from srcpart;
insert into srcpart_acid PARTITION (ds, hr) select * from srcpart;

alter table srcpart_acid partition(ds='2008-04-08',hr=='11') concatenate;
