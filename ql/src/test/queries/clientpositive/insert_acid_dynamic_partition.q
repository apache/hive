set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.exec.dynamic.partition.mode=nonstrict;

create table acid_dynamic(a int, b varchar(128)) partitioned by (ds string) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_dynamic partition (ds) select cint, cast(cstring1 as varchar(128)), cstring2 from alltypesorc where cint is not null and cint < 0 order by cint limit 5;

select * from acid_dynamic order by a,b;
