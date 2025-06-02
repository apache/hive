--! qt:dataset:src
--! qt:dataset:part

-- MV metadata is stored in Iceberg
-- SORT_QUERY_RESULTS
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
--! qt:replace:/(.*snapshotId=)\S+(\}.*)/$1#SnapshotId#$2/

set hive.explain.user=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.iceberg.materializedview.metadata.location=iceberg;
set hive.external.table.purge.default=true;


drop materialized view if exists mat_native;
drop table if exists tbl_ice_native;

create table tbl_ice_native(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='1');
insert into tbl_ice_native values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54);

explain
create materialized view mat_native stored by iceberg stored as orc tblproperties ('format-version'='1') as
select b, c from tbl_ice_native where c > 52;

create materialized view mat_native stored by iceberg stored as orc tblproperties ('format-version'='1', 'max-staleness-ms'='1000') as
select b, c from tbl_ice_native where c > 52;

select * from mat_native;


SHOW TABLES;
SHOW MATERIALIZED VIEWS;

show create table mat_native;
describe formatted mat_native;

drop materialized view mat_native;

create materialized view mat_native_orc stored by iceberg stored as orc tblproperties ('format-version'='1', 'max-staleness-ms'='1000') as
select b, c from tbl_ice_native where c > 52;

select * from mat_native_orc;

show create table mat_native_orc;
explain show create table mat_native_orc;
describe extended mat_native_orc;
explain describe formatted mat_native_orc;

describe formatted mat_native_orc;

insert into tbl_ice_native values (6, 'six', 60);

select * from mat_native_orc;

alter materialized view mat_native_orc rebuild;

select * from mat_native_orc;

create materialized view mat_native_orc_2 stored as orc tblproperties ('format-version'='1', 'max-staleness-ms'='1000') as
select b, c from tbl_ice_native where c > 52;


-- partitioned materialized view

create materialized view mat_native_orc_partitioned partitioned on (b) stored by iceberg stored as orc tblproperties ('format-version'='1') as
select b, c from tbl_ice_native where c > 52;

select * from mat_native_orc_partitioned;

describe formatted mat_native_orc_partitioned;

-- multiple tables

create external table tbl_ice_v2(d int, e string, f int) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into tbl_ice_v2 values (1, 'one v2', 50), (4, 'four v2', 53), (5, 'five v2', 54);

create materialized view mat_native_multiple_tables stored by iceberg as
select tbl_ice_native.b, tbl_ice_native.c, sum(tbl_ice_v2.f)
from tbl_ice_native
join tbl_ice_v2 on tbl_ice_native.a=tbl_ice_v2.d where tbl_ice_native.c > 52
group by tbl_ice_native.b, tbl_ice_native.c;

-- delete some records from a source table
delete from tbl_ice_v2 where d = 4;

-- plan should be insert overwrite
explain cbo
alter materialized view mat_native_multiple_tables rebuild;

alter materialized view mat_native_multiple_tables rebuild;

select * from mat_native_multiple_tables;

SHOW MATERIALIZED VIEWS;