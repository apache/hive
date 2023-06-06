-- MV data is stored by partitioned iceberg with partition spec
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
--! qt:replace:/(\s+current-snapshot-id\s+)\d+(\s*)/$1#SnapshotId#/
--! qt:replace:/(.*snapshotId=)\S+(\}.*)/$1#SnapshotId#$2/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
-- Mask the totalSize value as it can change at file format library update
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- SORT_QUERY_RESULTS

drop materialized view if exists mat1;
drop table if exists tbl_ice;

create table tbl_ice(a int, b string, c int) stored by iceberg stored as orc tblproperties ('format-version'='1');
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54);

create materialized view mat1 partitioned on spec (bucket(16, b), truncate(3, c)) stored by iceberg stored as orc tblproperties ('format-version'='1') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

describe formatted mat1;

select * from mat1;

create materialized view mat2 partitioned on spec (bucket(16, b), truncate(3, c)) stored by iceberg stored as orc tblproperties ('format-version'='2') as
select tbl_ice.b, tbl_ice.c from tbl_ice where tbl_ice.c > 52;

describe formatted mat2;
