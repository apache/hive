--! qt:dataset:src

set hive.metastore.dml.events=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


drop table test;
create table test(id int, name string);
insert into test values(1, 'aa'),(2,'bb');

drop table test3;
CREATE TABLE test3 stored as textfile LOCATION '${system:test.tmp.dir}/test2' tblproperties('transactional'='true', 'transactional_properties'='insert_only') AS SELECT * from test;