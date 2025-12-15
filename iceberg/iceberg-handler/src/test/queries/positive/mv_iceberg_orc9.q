-- MV metadata is stored in Iceberg
-- SORT_QUERY_RESULTS

set hive.explain.user=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.iceberg.materializedview.metadata.location=iceberg;
--set hive.server2.materializedviews.registry.impl=DUMMY;


drop materialized view if exists mat1;
drop table if exists tbl_ice;

create table tbl_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='1');
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54);

--explain
--create materialized view mat1 stored by iceberg stored as orc tblproperties ('format-version'='1') as
--select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

create materialized view mat1 stored by iceberg stored as orc tblproperties ('format-version'='1') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

select * from mat1;

show tables;

show materialized views;

show create table mat1;
describe formatted mat1;

select 1;

create materialized view mat1_orc stored as orc tblproperties ('format-version'='1') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

select * from mat1_orc;

show create table mat1_orc;
describe formatted mat1_orc;
