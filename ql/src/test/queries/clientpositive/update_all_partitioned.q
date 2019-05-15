--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table acid_uap(a int, b varchar(128)) partitioned by (ds string) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_uap partition (ds='today') select cint, cast(cstring1 as varchar(128)) as cs from alltypesorc where cint is not null and cint < 0 order by cint, cs limit 10;
insert into table acid_uap partition (ds='tomorrow') select cint, cast(cstring1 as varchar(128)) as cs from alltypesorc where cint is not null and cint > 10 order by cint, cs limit 10;

select a,b,ds from acid_uap order by a,b;

update acid_uap set b = 'fred';

select a,b,ds from acid_uap order by a,b;


