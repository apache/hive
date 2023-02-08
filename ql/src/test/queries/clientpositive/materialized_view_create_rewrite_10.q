-- Try to run incremental on a non-transactional MV in presence of delete operations
-- Compiler should fall back to full rebuild.

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.stats.autogather=false;

create table t1 (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1 values (1,1), (2,1), (3,3);

create materialized view mv1 as
select a, b from t1 where b = 1;

delete from t1 where a = 2;

explain cbo
alter materialized view mv1 rebuild;

explain
alter materialized view mv1 rebuild;

alter materialized view mv1 rebuild;

explain cbo
select a, b from t1 where b = 1;

select a, b from t1 where b = 1;
