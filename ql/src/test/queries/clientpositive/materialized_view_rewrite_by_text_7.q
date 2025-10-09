set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting=false;

create table t1(col0 int) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

create materialized view mat1 as
select col0 from t1 where col0 between 1 and 10 union select col0 from t1 where col0 = 20;

create materialized view mat2 as
select col0 from t1 where col0 > 15;

explain cbo
select col0 from
  (select col0 from t1 where col0 > 15) sub
where col0 = 20;

explain cbo
select col0 from
  (select col0 from t1 where col0 between 1 and 10 union select col0 from t1 where col0 = 20) sub
where col0 = 20;

drop materialized view mat1;
drop materialized view mat2;
