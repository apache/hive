-- Calcite based automatic query rewrite should be disabled inside view creation
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting.sql=false;

create table t1(col0 int) STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into t1(col0) values (1),(3),(10),(NULL);

create materialized view mv1 as
select * from t1 where col0 > 2;

explain cbo
create view v1 as
select sub.* from (select * from t1 where col0 > 2) sub
where sub.col0 = 10;

create view v1 as
select sub.* from (select * from t1 where col0 > 2) sub
where sub.col0 = 10;

explain cbo
select * from v1;

select * from v1;
