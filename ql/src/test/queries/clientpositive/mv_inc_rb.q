set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.vectorized.execution.enabled=false;
set hive.materializedview.rewriting.sql=false;

create table t1(a int, b varchar(128), c float) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(a,b, c) values (1, 'one', 1.1), (2, 'two', 2.2), (NULL, NULL, NULL);
insert into t1(a,b, c) values (3, 'three', 3.3);
insert into t1(a,b, c) values (4, 'four', 4.4);

create materialized view mat1 stored as orc TBLPROPERTIES ('transactional'='true') as
select a,b,c from t1;


-- do some changes on source table data
delete from t1 where a = 4;
delete from t1 where a = 3;
delete from t1 where b is NULL;

insert into t1(a,b, c) values (2, 'two', 2.2), (2, 'two', 2.2), (5, 'five', 5.5);

-- the view should not change
select * from mat1;

-- rebuild the view (incrementally)
explain cbo
alter materialized view mat1 rebuild;
explain
alter materialized view mat1 rebuild;
alter materialized view mat1 rebuild;

-- the view should be used
explain cbo
select a,b,c from t1 where a > 1;

select * from mat1;


-- make additional changes on source and refresh the view: new row should be inserted to the view
insert into t1(a,b, c) values (NULL, NULL, NULL);

explain
alter materialized view mat1 rebuild;
alter materialized view mat1 rebuild;

select * from mat1;
