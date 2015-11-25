set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table acid_insertsort(a int, b varchar(128)) clustered by (a) sorted by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_insertsort values (1, 'abc'),(2, 'def');
