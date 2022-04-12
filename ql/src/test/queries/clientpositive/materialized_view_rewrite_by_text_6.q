set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting=false;

create table t1(col0 int) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

create table t2(col0 int) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

insert into t1(col0) values (1), (NULL);
insert into t2(col0) values (1), (2), (3), (NULL);

create materialized view mat1 as
select col0 from t1 where col0 = 1 union select col0 from t1 where col0 = 2;

create materialized view mat2 as
select col0 from t1 where col0 = 3;

-- This query is not rewritten because the MV mat2 is applicable for Calcite based rewrite algorithm but that is turned off in this test case.
explain cbo
select col0 from t2 where exists (
 select col0 from t1 where col0 = 3);

-- These queries should be rewritten because only sql text based rewrite is applicable for MV mat1
explain cbo
select col0 from t2 where exists (select col0 from t1 where col0 = 1 union select col0 from t1 where col0 = 2);

explain cbo
select col0 from t2 where col0 in (select col0 from t1 where col0 = 1 union select col0 from t1 where col0 = 2);

drop materialized view mat1;
drop materialized view mat2;
