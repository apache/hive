set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE acid_vectorized(a INT, b STRING, c float) CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid_vectorized select cint, cstring1, cfloat from alltypesorc where cint is not null order by cint limit 10;
set hive.vectorized.execution.enabled=true;
select a,b from acid_vectorized order by a;
select a,c from acid_vectorized order by a;
select b,c from acid_vectorized order by b;
