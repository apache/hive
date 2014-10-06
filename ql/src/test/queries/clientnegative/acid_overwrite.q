set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.enforce.bucketing=true;

create table acid_uanp(a int, b varchar(128)) clustered by (a) into 2 buckets stored as orc;

insert into table acid_uanp select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint < 0 order by cint limit 10;
insert overwrite table acid_uanp select cint, cast(cstring1 as varchar(128)) from alltypesorc;
