set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled=true;

CREATE TABLE acid_vectorized(a INT, b STRING) CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid_vectorized select cint, cstring1 from alltypesorc where cint is not null order by cint limit 10;
set hive.vectorized.execution.enabled=true;
insert into table acid_vectorized values (1, 'bar');
set hive.vectorized.execution.enabled=true;
update acid_vectorized set b = 'foo' where b = 'bar';
set hive.vectorized.execution.enabled=true;
delete from acid_vectorized where b = 'foo';
set hive.vectorized.execution.enabled=true;
select a, b from acid_vectorized order by a, b;
