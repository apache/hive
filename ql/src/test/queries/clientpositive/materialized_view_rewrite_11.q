set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting.sql=false;

create table t1(col0 int) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

create materialized view mat1 as
select * from t1 where col0 = 1
union
select * from t1 where col0 = 2;

-- MV is not used because sql text based is disabled calcite based does not support UNION operator in view definition.
explain cbo
select * from t1 where col0 = 1
union
select * from t1 where col0 = 2;

explain
select * from t1 where col0 = 1
union
select * from t1 where col0 = 2;

drop materialized view mat1;
