set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting=false;

create table t1(col0 int) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

create materialized view mat1 as
select * from t1 where col0 = 1
union
select * from t1 where col0 = 2;

explain cbo
select * from t1 where col0 = 1
union
select * from t1 where col0 = 2;


set hive.materializedview.rewriting.sql=false;

explain cbo
select * from t1 where col0 = 1
union
select * from t1 where col0 = 2;

drop materialized view mat1;
