-- MV is stored by iceberg v3 and has aggregate. Source tables are native ACID tables.
-- Source tables have inserts and deletes after MV creation.
-- SORT_QUERY_RESULTS

set hive.explain.user=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop materialized view if exists mat1;
drop table if exists t1;
drop table if exists t2;

create table t1 (a int, b string, c int) stored as orc tblproperties ('transactional'='true');
create table t2 (d int, e string, f int) stored as orc tblproperties ('transactional'='true');

insert into t1 values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54);
insert into t2 values (1, 'x', 10), (2, 'x', 20), (2, 'y', 21), (4, 'x', 40), (4, 'y', 41), (5, 'x', 50);

create materialized view mat1 stored by iceberg stored as orc tblproperties ('format-version'='3') as
select t1.b, sum(t2.f), count(*)
from t1 join t2 on t1.a = t2.d
where t1.c > 50
group by t1.b;

-- insert and delete some records in the source tables
insert into t1 values (6, 'six', 55);
insert into t2 values (6, 'x', 60), (5, 'y', 51);
delete from t1 where a = 4;
delete from t2 where d = 2 and e = 'y';

explain
alter materialized view mat1 rebuild;

alter materialized view mat1 rebuild;

-- MV content must match the defining query
select * from mat1;
