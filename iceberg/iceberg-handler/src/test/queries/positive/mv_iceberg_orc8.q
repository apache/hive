-- MV source table has varchar column.
-- SORT_QUERY_RESULTS

set hive.explain.user=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set iceberg.mr.schema.auto.conversion=true;

create table t1 (a int, b varchar(256), c char(100), d int not null) stored as orc tblproperties ('transactional'='true');

insert into t1 values (1, 'Alfred', 'Alfred', 1);

create materialized view mat1 stored by iceberg stored as orc tblproperties ('format-version'='2') as
select b, c, d, sum(a) from t1 group by b, c, d;

insert into t1 values (4, 'Jane', 'Jane', 4);

explain
alter materialized view mat1 rebuild;

alter materialized view mat1 rebuild;

select * from mat1;
