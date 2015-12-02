set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table acid_dwhp(a int, b varchar(128)) partitioned by (ds string) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_dwhp partition (ds='today') select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null and cint < 0 order by cint limit 10;
insert into table acid_dwhp partition (ds='tomorrow') select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null and cint > -10000000 order by cint limit 10;

select a,b,ds from acid_dwhp order by a, ds;

delete from acid_dwhp where ds = 'today';

select * from acid_dwhp order by a, ds;
