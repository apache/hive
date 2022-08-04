-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create external table tbl_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into tbl_ice values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54);

select * from tbl_ice;


create materialized view mat1 as
select b, c from tbl_ice where c > 52;

select * from mat1;

explain cbo
select b, c from tbl_ice where c > 52;

insert into tbl_ice values (111, 'one', 55), (333, 'two', 56);

explain cbo
select b, c from tbl_ice where c > 52;

explain cbo
alter materialized view mat1 rebuild;

alter materialized view mat1 rebuild;

select * from mat1;

explain cbo
select b, c from tbl_ice where c > 52;
