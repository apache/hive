set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table acid_dwp(a int, b varchar(128)) partitioned by (ds string) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_dwp partition (ds='today') select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null and cint < 0 order by cint limit 10;
insert into table acid_dwp partition (ds='tomorrow') select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null and cint > -10000000 order by cint limit 10;

select a,b,ds from acid_dwp order by a, ds;

delete from acid_dwp where a = '-1071363017';

select * from acid_dwp order by a, ds;
