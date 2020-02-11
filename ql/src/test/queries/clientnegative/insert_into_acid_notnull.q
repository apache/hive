set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table acid_uami(i int,
                 de decimal(5,2) not null enforced,
                 vc varchar(128) not null enforced) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');


insert into table acid_uami select 1, null, null;
