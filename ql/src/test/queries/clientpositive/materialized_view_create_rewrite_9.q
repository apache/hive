-- Test Incremental rebuild of materialized view with aggregate and count(*) when
-- 1) source tables have delete operations since last rebuild.
-- 2) a source table is insert only.
-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting.sql=false;

create table t1(a char(15), b int, c int) stored as orc TBLPROPERTIES ('transactional'='true');
create table t2(a char(15), b int) stored as orc TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

insert into t1(a, b, c) values
('update', 1, 1), ('update', 2, 1),
('null_update', null, 1), ('null_update', null, 2);
insert into t1(a, b, c) values ('remove', 3, 1), ('null_remove', null, 1);
insert into t1(a, b, c) values ('sum0', 0, 1), ('sum0', 0, 2);

insert into t2(a, b) values
('update', 10),
('null_update', null);
insert into t2(a, b) values ('remove', 30), ('null_remove', null);
insert into t2(a, b) values ('sum0', 0);

-- Aggregate with count(*): incremental rebuild should be triggered even if there were deletes from source table
create materialized view mat1 stored as orc TBLPROPERTIES ('transactional'='true') as
select t1.a, sum(t1.b), count(*) from t1
join t2 on (t1.a = t2.a)
group by t1.a;


explain cbo
select t1.a, sum(t1.b) from t1
join t2 on (t1.a = t2.a)
group by t1.a;

-- do some changes on source table data
delete from t1 where b = 1;
delete from t1 where a like '%remove';
delete from t1 where c = 2;

insert into t1(a,b,c) values
('update', 5, 1),
('add', 5, 1),
('add/remove', 0, 0),
('null_update', null, 0),
('null_add', null, 0),
('null_add/remove', null, 0);

insert into t2(a,b) values
('add', 15),
('add/remove', 0),
('null_add', null),
('null_add/remove', null);

delete from t1 where a like '%add/remove';

-- view can not be used
explain cbo
select t1.a, sum(t1.b) from t1
join t2 on (t1.a = t2.a)
group by t1.a;


-- rebuild the view (incrementally)
explain cbo
alter materialized view mat1 rebuild;
explain
alter materialized view mat1 rebuild;
alter materialized view mat1 rebuild;

-- the view should be up to date and used
explain cbo
select t1.a, sum(t1.b) from t1
join t2 on (t1.a = t2.a)
group by t1.a;

select t1.a, sum(t1.b) from t1
join t2 on (t1.a = t2.a)
group by t1.a;

drop materialized view mat1;

select t1.a, sum(t1.b) from t1
join t2 on (t1.a = t2.a)
group by t1.a;
