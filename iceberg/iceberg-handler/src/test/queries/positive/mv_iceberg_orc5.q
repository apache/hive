-- MV source tables are iceberg and MV has aggregate. It also has avg which is calculated from sum and count.
-- SORT_QUERY_RESULTS
--! qt:replace:/(.*fromVersion=\[)\S+(\].*)/$1#Masked#$2/
--! qt:replace:/(\s+Version\sinterval\sfrom\:\s+)\d+(\s*)/$1#Masked#/

set hive.explain.user=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists tbl_ice;

create external table tbl_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='1');
create external table tbl_ice_v2(d int, e string, f int) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into tbl_ice values (1, 'one', 50), (4, 'four', 53), (5, 'five', 54);
insert into tbl_ice_v2 values (1, 'one v2', 50), (4, 'four v2', 53), (5, 'five v2', 54);

create materialized view mat2 stored by iceberg stored as orc tblproperties ('format-version'='2') as
select tbl_ice.b, tbl_ice.c, sum(tbl_ice_v2.f), count(tbl_ice_v2.f), avg(tbl_ice_v2.f)
from tbl_ice
join tbl_ice_v2 on tbl_ice.a=tbl_ice_v2.d where tbl_ice.c > 52
group by tbl_ice.b, tbl_ice.c;

-- insert some new records to the source tables
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54);
insert into tbl_ice_v2 values (1, 'one v2', 50), (4, 'four v2', 53), (5, 'five v2', 54);

explain cbo
alter materialized view mat2 rebuild;
explain
alter materialized view mat2 rebuild;
alter materialized view mat2 rebuild;

select * from mat2;
