-- CREATE MATERIALIZED VIEW should fail if it references a temp table
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create temporary table t1 (a int) stored as orc tblproperties ('transactional'='true');
create table t2 (a int) stored as orc tblproperties ('transactional'='true');

create materialized view mv1 as
select t2.a from t1
join t2 on (t1.a = t2.a);

drop materialized view mv1;
