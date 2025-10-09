-- MV data is stored by iceberg v1
-- SORT_QUERY_RESULTS
--! qt:replace:/(.*fromVersion=\[)\S+(\].*)/$1#Masked#$2/

set hive.explain.user=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop materialized view if exists mat1;
drop table if exists tbl_ice;

create table tbl_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='1');
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54);

explain
create materialized view mat1 stored by iceberg stored as orc tblproperties ('format-version'='1') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

create materialized view mat1 stored by iceberg stored as orc tblproperties ('format-version'='1') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

select * from mat1;

alter materialized view mat1 disable rewrite;

-- no rewrite
explain cbo
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

alter materialized view mat1 enable rewrite;

-- rewrite
explain cbo
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

insert into tbl_ice values (10, 'ten', 60);

explain cbo
alter materialized view mat1 rebuild;

alter materialized view mat1 rebuild;

select * from mat1;
