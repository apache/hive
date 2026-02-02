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

SHOW MATERIALIZED VIEWS;
