set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.vectorized.execution.enabled=false;
set hive.materializedview.rewriting.sql=false;

create table t1(a int, b varchar(128), c float) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(a,b, c) values (1, 'one', 1.1), (2, 'two', 2.2);
insert into t1(a,b, c) values (3, 'three', 3.3);
insert into t1(a,b, c) values (4, 'four', 4.4);

create materialized view mat1 stored as orc TBLPROPERTIES ('transactional'='true') as
select a,b,c from t1 where a > 1;


delete from t1 where a = 4;
delete from t1 where a = 3;

insert into t1(a,b, c) values (5, 'five', 5.5);

explain cbo
alter materialized view mat1 rebuild;
explain
alter materialized view mat1 rebuild;
alter materialized view mat1 rebuild;

explain cbo
select a,b,c from t1 where a > 1;

select * from mat1;
