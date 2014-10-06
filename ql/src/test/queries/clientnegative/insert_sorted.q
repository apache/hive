set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table acid_insertsort(a int, b varchar(128)) clustered by (a) sorted by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_insertsort select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null order by cint limit 10;
