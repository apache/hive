set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table acid_insertsort(a int, b varchar(128)) clustered by (a) sorted by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

update acid_insertsort set b = 'fred' where b = 'bob';
