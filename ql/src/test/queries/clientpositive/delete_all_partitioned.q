set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.enforce.bucketing=true;

create table acid_dap(a int, b varchar(128)) partitioned by (ds string) clustered by (a) into 2 buckets stored as orc;

insert into table acid_dap partition (ds='today') select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null and cint < 0 order by cint limit 10;
insert into table acid_dap partition (ds='tomorrow') select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null and cint > 1000 order by cint limit 10;

select a,b,ds from acid_dap order by a,b;

delete from acid_dap;

select * from acid_dap;
