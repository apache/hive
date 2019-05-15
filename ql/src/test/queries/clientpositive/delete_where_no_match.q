--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table acid_dwnm(a int, b varchar(128)) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_dwnm select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null order by cint limit 10;

select * from acid_dwnm order by a;

delete from acid_dwnm where b = 'nosuchvalue';

select a,b from acid_dwnm order by b;


