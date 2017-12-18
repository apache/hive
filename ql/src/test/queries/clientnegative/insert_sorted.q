set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table mm_insertsort(a int, b varchar(128)) clustered by (a) sorted by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true', "transactional_properties"="insert_only");
insert into mm_insertsort values (1, '1'),(2, '2');

create table acid_insertsort(a int, b varchar(128)) clustered by (a) sorted by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
