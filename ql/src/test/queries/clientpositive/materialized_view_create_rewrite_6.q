set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting.sql=false;

create table t1(a char(15), b int, c int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(a, b, c) values
('update', 1, 1), ('update', 2, 1),
('null_update', null, 1), ('null_update', null, 2);
insert into t1(a, b, c) values ('remove', 3, 1), ('null_remove', null, 1);
insert into t1(a, b, c) values ('sum0', 0, 1), ('sum0', 0, 2);

-- Aggregate with count(*): incremental rebuild should be triggered even if there were deletes from source table
create materialized view mat1 stored as orc TBLPROPERTIES ('transactional'='true') as
select a, sum(b), count(*) from t1 group by a;


explain cbo
select a, sum(b) from t1 group by a order by a;

-- do some changes on source table data
delete from t1 where b = 1;
delete from t1 where a = 'remove';
delete from t1 where c = 2;

insert into t1(a,b,c) values
('update', 5, 1),
('add', 5, 1),
('add/remove', 0, 0),
('null_update', null, 0),
('null_add', null, 0),
('null_add/remove', null, 0);

delete from t1 where a like 'add/remove';

-- view can not be used
explain cbo
select a, sum(b) from t1 group by a order by a;


-- rebuild the view (incrementally)
explain cbo
alter materialized view mat1 rebuild;
explain
alter materialized view mat1 rebuild;
alter materialized view mat1 rebuild;

-- the view should be up to date and used
explain cbo
select a, sum(b) from t1 group by a;

select a, sum(b) from t1 group by a order by a;

drop materialized view mat1;

select a, sum(b) from t1 group by a order by a;
