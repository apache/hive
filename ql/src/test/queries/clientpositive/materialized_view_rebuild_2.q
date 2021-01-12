set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table t1(col0 int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(col0) values(1);

create materialized view mat1 as
select col0 from t1 where col0 = 1;

explain
alter materialized view mat1 rebuild;

explain formatted
alter materialized view mat1 rebuild;

alter materialized view mat1 rebuild;

insert into t1(col0) values(1);

explain
alter materialized view mat1 rebuild;

alter materialized view mat1 rebuild;

drop materialized view mat1;
