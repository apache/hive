-- Calcite based automatic query rewrite is disabled because of the unsupported union operator
-- However text based could work.

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table t1(col0 int) STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into t1(col0) values (0),(1),(3),(10),(NULL);

create materialized view mv1 as
select * from t1 where col0 > 2 union select * from t1 where col0 = 0;

-- Both automatic query rewrite is algorithm should be disabled inside view creation.
explain cbo
create view v1 as
select sub.* from (select * from t1 where col0 > 2 union select * from t1 where col0 = 0) sub
where sub.col0 = 10;

create view v1 as
select sub.* from (select * from t1 where col0 > 2 union select * from t1 where col0 = 0) sub
where sub.col0 = 10;

explain cbo
select * from v1;

select * from v1;
