-- MV source tables are iceberg and MV has aggregate.
-- SORT_QUERY_RESULTS
--! qt:replace:/(.*fromVersion=\[)\S+(\].*)/$1#Masked#$2/
--! qt:replace:/(\s+Version\sinterval\sfrom\:\s+)\d+(\s*)/$1#Masked#/

set hive.explain.user=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.stats.column.autogather=false;

drop table if exists tbl_ice;

create external table tbl_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='1');

insert into tbl_ice values (1, 'one', 50), (4, 'four', 53), (5, 'five', 54);

create materialized view mat1 stored by iceberg stored as orc tblproperties ('format-version'='2') as
select a, count(c)
from tbl_ice
group by a;

-- insert some new records to the source tables
insert into tbl_ice values (1, 'one', 50);

select * from mat1;

explain cbo
alter materialized view mat1 rebuild;
explain
alter materialized view mat1 rebuild;
alter materialized view mat1 rebuild;

select * from mat1;
