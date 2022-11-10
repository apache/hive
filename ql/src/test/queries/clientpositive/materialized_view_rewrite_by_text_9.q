set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting=false;

create table t1(col0 int) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

create table t2(col0 int) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

create materialized view mat1 as
select col0 from t1 where col0 = 1 union select col0 from t1 where col0 = 2;

-- View can be used -> rewrite
explain cbo
select col0 from t2 where col0 in (select col0 from t1 where col0 = 1 union select col0 from t1 where col0 = 2);

insert into t1(col0) values (2);

-- View can not be used since it is outdated
explain cbo
select col0 from t2 where col0 in (select col0 from t1 where col0 = 1 union select col0 from t1 where col0 = 2);
