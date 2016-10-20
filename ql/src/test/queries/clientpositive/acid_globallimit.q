set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.fetch.task.conversion=none;
set hive.limit.optimize.enable=true;
set hive.mapred.mode=nonstrict;
-- Global Limit optimization does not work with ACID table. Make sure to skip it for ACID table.
CREATE TABLE acidtest1(c1 INT, c2 STRING)
CLUSTERED BY (c1) INTO 3 BUCKETS
STORED AS ORC
TBLPROPERTIES ("transactional"="true");

insert into table acidtest1 select cint, cstring1 from alltypesorc where cint is not null order by cint;

select cast (c1 as string) from acidtest1 order by c1 limit 10;

drop table acidtest1;
