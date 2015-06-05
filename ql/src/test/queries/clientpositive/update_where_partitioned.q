set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table acid_uwp(a int, b varchar(128)) partitioned by (ds string) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_uwp partition (ds='today') select cint, cast(cstring1 as varchar(128)) as cs from alltypesorc where cint is not null and cint < 0 order by cint, cs limit 10;
insert into table acid_uwp partition (ds='tomorrow') select cint, cast(cstring1 as varchar(128)) as cs from alltypesorc where cint is not null and cint > 100 order by cint, cs limit 10;

select a,b,ds from acid_uwp order by a, ds, b;

update acid_uwp set b = 'fred' where b = 'k17Am8uPHWk02cEf1jet';

select * from acid_uwp order by a, ds, b;


