set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table t1(col0 int, col1 int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(col0, col1) values
(1,1),
(2,1),
(2,2);

create materialized view mat1 as
select col0, sum(col1) from t1 group by col0;

explain
alter materialized view mat1 rebuild;

alter materialized view mat1 rebuild;

insert into t1(col0, col1) values(1,10);

explain
alter materialized view mat1 rebuild;

alter materialized view mat1 rebuild;

drop materialized view mat1;
