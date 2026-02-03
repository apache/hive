-- MV metadata is stored in Iceberg
-- SORT_QUERY_RESULTS
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
--! qt:replace:/(.*snapshotId=)\S+(\}.*)/$1#SnapshotId#$2/

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

create materialized view mat1 stored by iceberg stored as orc tblproperties ('format-version'='1', 'max-staleness-ms'='1000') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

select * from mat1;

show tables;

SHOW MATERIALIZED VIEWS;

show create table mat1;
describe formatted mat1;

drop materialized view mat1;

create materialized view mat1_orc stored by iceberg stored as orc tblproperties ('format-version'='1', 'max-staleness-ms'='1000') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

select * from mat1_orc;

show create table mat1_orc;
explain show create table mat1_orc;
describe extended mat1_orc;
explain describe formatted mat1_orc;

describe formatted mat1_orc;

insert into tbl_ice values (6, 'six', 60);

select * from mat1_orc;

select 1;

alter materialized view mat1_orc rebuild;

select * from mat1_orc;

select 1;

create materialized view mat2_orc stored as orc tblproperties ('format-version'='1', 'max-staleness-ms'='1000') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;


-- partitioned materialized view

create materialized view mat1_orc_partitioned partitioned on (b) stored by iceberg stored as orc tblproperties ('format-version'='1') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

select * from mat1_orc_partitioned;

describe formatted mat1_orc_partitioned;

-- multiple tables

create external table tbl_ice_v2(d int, e string, f int) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into tbl_ice_v2 values (1, 'one v2', 50), (4, 'four v2', 53), (5, 'five v2', 54);

create materialized view mat1_multiple_tables stored by iceberg as
select tbl_ice.b, tbl_ice.c, sum(tbl_ice_v2.f)
from tbl_ice
join tbl_ice_v2 on tbl_ice.a=tbl_ice_v2.d where tbl_ice.c > 52
group by tbl_ice.b, tbl_ice.c;

-- delete some records from a source table
delete from tbl_ice_v2 where d = 4;

-- plan should be insert overwrite
-- explain cbo
-- alter materialized view mat1_multiple_tables rebuild;

-- alter materialized view mat1_multiple_tables rebuild;

-- select * from mat1_multiple_tables;

SHOW MATERIALIZED VIEWS;
