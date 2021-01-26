set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.vectorized.execution.enabled=false;

create table t1(a int, b varchar(128)) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1(a,b) values (1, 'foo'), (2, 'bar');

delete from t1 where a = 1;

select t1.ROW__IS__DELETED, t1.ROW__ID, * from t1;

insert into t1(a,b) values (3, 'one'), (4, 'two'), (4, 'three'), (5, 'four');

select /*+ FETCH_DELETED_ROWS*/ t1.ROW__IS__DELETED, t1.ROW__ID, * from t1;

update t1
set b = 'updated'
where a = 3;

select /*+ FETCH_DELETED_ROWS*/ t1.ROW__IS__DELETED, t1.ROW__ID, * from t1;

select /*+ FETCH_DELETED_ROWS*/ t1.ROW__IS__DELETED, t1.ROW__ID, * from t1 order by a;


create table t2(a int, c float) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t2(a,c) values (1, 1.0), (2, 2.0);

insert into t2(a,c) values (3, 3.3), (4, 4.4), (4, 4.5), (5, 5.5);

select t1.ROW__ID, t1.*, t2.ROW__ID, t2.* from t1
join t2 on t1.a = t2.a;

select /*+ FETCH_DELETED_ROWS*/ t1.ROW__IS__DELETED, t1.ROW__ID, t1.*, t2.ROW__IS__DELETED, t2.ROW__ID, t2.* from t1
join t2 on t1.a = t2.a;

delete from t2 where a in (1, 4);

select t1.ROW__ID, t1.*, t2.ROW__ID, t2.* from t1
join t2 on t1.a = t2.a;

select /*+ FETCH_DELETED_ROWS*/ t1.ROW__IS__DELETED, t1.ROW__ID, t1.*, t2.ROW__IS__DELETED, t2.ROW__ID, t2.* from t1
join t2 on t1.a = t2.a;
