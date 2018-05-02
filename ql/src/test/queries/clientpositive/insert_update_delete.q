--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table acid_iud(a int, b varchar(128)) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_iud select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint < 0 order by cint limit 10;

select a,b from acid_iud order by a;

update acid_iud set b = 'fred';

select a,b from acid_iud order by a;

delete from acid_iud;

select a,b from acid_iud order by a;

