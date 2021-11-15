--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


CREATE TABLE acid_vectorized_part(a INT, b STRING) partitioned by (ds string) CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid_vectorized_part partition (ds = 'today') select cint, cstring1 from alltypesorc where cint is not null order by cint limit 10;
insert into table acid_vectorized_part partition (ds = 'tomorrow') select cint, cstring1 from alltypesorc where cint is not null order by cint limit 10;
set hive.vectorized.execution.enabled=true;
select * from acid_vectorized_part order by a, b, ds;
