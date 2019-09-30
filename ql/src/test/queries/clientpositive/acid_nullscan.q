--! qt:dataset:alltypesorc

set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.vectorized.execution.enabled=true;

CREATE TABLE acid_vectorized_n1(a INT, b STRING) CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid_vectorized_n1 select cint, cstring1 from alltypesorc where cint is not null order by cint limit 10;
insert into table acid_vectorized_n1 values (1, 'bar');


explain extended
select sum(a) from acid_vectorized_n1 where false;

select sum(a) from acid_vectorized_n1 where false;

