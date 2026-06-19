-- Test handling Sum0 aggregate function when rewriting insert overwrite MV rebuild plan to incremental

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting.sql=false;

create table t1(a char(15), b int, c int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(a, b, c) values
('group0', 1, 1), ('group0', 2, 1),
('group_null', null, null), ('group_null', null, null),
(null, null, null);
insert into t1(a, b, c) values ('group1', 0, 1), ('group1', 0, 2);

-- Aggregate with count(*): incremental rebuild should be triggered even if there were deletes from source table
create materialized view mat1 stored as orc TBLPROPERTIES ('transactional'='true') as
select t1.a, count(*) from t1
group by t1.a;


-- do some changes on source table data
insert into t1(a,b,c) values
('group0', 5, 1),
('group_null', null, null),
(null, null, null),
('group_add', 5, 1);


-- view can not be used
explain cbo
select t1.a, count(*) from t1
group by t1.a;


-- rebuild the view (full)
explain cbo
alter materialized view mat1 rebuild;
explain
alter materialized view mat1 rebuild;
alter materialized view mat1 rebuild;

-- the view should be up to date and used
explain cbo
select t1.a, count(*) from t1
group by t1.a
order by t1.a;

select t1.a, count(*) from t1
group by t1.a
order by t1.a;

drop materialized view mat1;

select t1.a, count(*) from t1
group by t1.a
order by t1.a;
