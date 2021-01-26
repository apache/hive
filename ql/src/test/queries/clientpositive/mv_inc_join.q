set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.vectorized.execution.enabled=false;
set hive.materializedview.rewriting.sql=false;

create table t1(a int, b varchar(128), c float, d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(a, c, d) values (1, 1.1, 101), (2, 2.2, 102);
insert into t1(a, c, d) values (3, 3.3, 103);
insert into t1(a, c, d) values (4, 4.4, 104);

create table t2(a int, b varchar(128)) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t2(a, b) values (1, 'one'), (2, 'two'), (3, 'three');

select t1.a, t2.b, t1.c from t1 inner join t2 ON t1.a = t2.a;

create materialized view mat1 stored as orc TBLPROPERTIES ('transactional'='true') as
select t1.a, t2.b, t1.c from t1 inner join t2 ON t1.a = t2.a;


delete from t1 where a = 4;
delete from t1 where a = 3;

insert into t1(a, c, d) values (2, 2.2, 202), (2, 2.2, 202), (5, 5.5, 505);
insert into t2(a, b) values (5, 'five');

explain cbo
alter materialized view mat1 rebuild;
explain
alter materialized view mat1 rebuild;
alter materialized view mat1 rebuild;

explain cbo
select t1.a, t2.b, t1.c from t1 inner join t2 ON t1.a = t2.a;

select * from mat1;

drop materialized view mat1;

select t1.a, t2.b, t1.c from t1 inner join t2 ON t1.a = t2.a;
