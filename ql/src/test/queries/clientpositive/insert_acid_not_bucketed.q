set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table acid_notbucketed_n0(a int, b varchar(128)) stored as orc;

insert into table acid_notbucketed_n0 select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null order by cint limit 10;

select * from acid_notbucketed_n0;
