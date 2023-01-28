-- MV data is stored by iceberg v2

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop materialized view if exists mat1;
drop table if exists tbl_ice;

create external table tbl_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54);

create materialized view mat1 stored by iceberg stored as orc tblproperties ('format-version'='2') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

delete from tbl_ice where a = 4;

alter materialized view mat1 rebuild;

select * from mat1;
