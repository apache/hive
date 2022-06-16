-- Test Incremental rebuild when stats autogather is off. In this case fall back to check the completed txn log.

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting.sql=false;

create table t1(a char(15), b int, c int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(a, b, c) values ('first', 0, 1), ('first', 0, 2);

create materialized view mat1 stored as orc TBLPROPERTIES ('transactional'='true') as
select a, b, c from t1 where c = 1;

explain cbo
select a, b, c from t1 where c = 1;

set hive.stats.autogather=false;

update t1 set a = 'changed' where c = 1;

explain cbo
alter materialized view mat1 rebuild;

-- This rebuilds only basic stats but txn stats are still invalid.
analyze table t1 compute statistics;

explain cbo
alter materialized view mat1 rebuild;
