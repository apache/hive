-- Test rewrite when query has aggregate can be derived from MV aggregates: avg(x) = sum(x)/count(x)

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.stats.autogather=false;

create table t1 (a int, b decimal(3,2)) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1 values (1,1), (2,1), (3,3);

create materialized view mv1 as
select a, sum(b), count(b) from t1 group by a;

explain cbo
select a, avg(b) from t1 group by a;

select a, avg(b) from t1 group by a;
