set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table t1(a int, b varchar(128)) stored as orc tblproperties ('transactional'='true');

insert into t1(a,b) values (1, 'one'), (2, 'two');

delete from t1 where a = 1;

insert into t1(a,b) values (3, 'three'), (4, 'four'), (4, 'four again'), (5, 'five');

explain vectorization
select t1.ROW__IS__DELETED, * from t1('acid.fetch.deleted.rows'='true') order by a;

select t1.ROW__IS__DELETED, * from t1('acid.fetch.deleted.rows'='true') order by a;


update t1
set b = 'updated'
where a = 3;

select t1.ROW__IS__DELETED, * from t1('acid.fetch.deleted.rows'='true') order by a;


create table t2(a int, c float) stored as orc tblproperties ('transactional'='true');

insert into t2(a,c) values (1, 1.0), (2, 2.0), (3, 3.3), (4, 4.4), (4, 4.5), (5, 5.5);

select t1.*, t2.* from t1
join t2 on t1.a = t2.a
order by t1.a;

select t1.ROW__IS__DELETED, t1.*, t2.ROW__IS__DELETED, t2.* from t1('acid.fetch.deleted.rows'='true')
join t2('acid.fetch.deleted.rows'='true') on t1.a = t2.a
order by t1.a;

delete from t2 where a in (1, 4);

select t1.*, t2.* from t1
join t2 on t1.a = t2.a
order by t1.a;

explain vectorization
select t1.ROW__IS__DELETED, t1.ROW__ID.writeId, t1.*, t2.ROW__IS__DELETED, t2.ROW__ID.writeId, t2.* from t1('acid.fetch.deleted.rows'='true')
join t2('acid.fetch.deleted.rows'='true') on t1.a = t2.a
order by t1.a;

select t1.ROW__IS__DELETED, t1.ROW__ID.writeId, t1.*, t2.ROW__IS__DELETED, t2.ROW__ID.writeId, t2.* from t1('acid.fetch.deleted.rows'='true')
join t2('acid.fetch.deleted.rows'='true') on t1.a = t2.a
order by t1.a;

set hive.transactional.events.mem=0;

select t1.ROW__IS__DELETED, t1.ROW__ID.writeId, t1.*, t2.ROW__IS__DELETED, t2.ROW__ID.writeId, t2.* from t1('acid.fetch.deleted.rows'='true')
join t2('acid.fetch.deleted.rows'='true') on t1.a = t2.a
order by t1.a;
