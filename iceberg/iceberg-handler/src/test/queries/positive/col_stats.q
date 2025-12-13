-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

-- Create source table
drop table if exists src_ice;
create external table src_ice(
    a int,
    b string,
    c int)
stored by iceberg;

insert into src_ice values
    (1, 'one', 50),
    (2, 'two', 51),
    (2, 'two', 51),
    (2, 'two', 51),
    (3, 'three', 52),
    (4, 'four', 53),
    (5, 'five', 54),
    (111, 'one', 55),
    (333, 'two', 56);

-- Test hive.iceberg.stats.source = iceberg
set hive.iceberg.stats.source=iceberg;


-- Test NON-PARTITIONED table with hive.iceberg.stats.source=iceberg
drop table if exists tbl_ice_puffin;
create external table tbl_ice_puffin(
    a int,
    b string,
    c int)
stored by iceberg;

insert into tbl_ice_puffin select * from src_ice;
insert into tbl_ice_puffin select * from src_ice;

select count(*) from tbl_ice_puffin;
explain select min(a), count(distinct b), max(c) from tbl_ice_puffin;
desc formatted tbl_ice_puffin B;

update tbl_ice_puffin
    set b='two' where b='one' or b='three';

analyze table tbl_ice_puffin compute statistics for columns;

select count(*) from tbl_ice_puffin;
explain select min(a), count(distinct b), max(c) from tbl_ice_puffin;
desc formatted tbl_ice_puffin B;


-- Test PARTITIONED table with hive.iceberg.stats.source=iceberg
drop table tbl_ice_puffin;
create external table tbl_ice_puffin(
    a int,
    b string
)
partitioned by (c int)
stored by iceberg;

insert overwrite table tbl_ice_puffin select * from src_ice;
delete from tbl_ice_puffin where a <= 2;

analyze table tbl_ice_puffin compute statistics for columns A, C;

select count(*) from tbl_ice_puffin;
explain select min(a), max(c) from tbl_ice_puffin;
desc formatted tbl_ice_puffin C;


-- Test hive.iceberg.stats.source is empty
set hive.iceberg.stats.source= ;

drop table tbl_ice_puffin;
create external table tbl_ice_puffin(
    a int,
    b string,
    c int)
stored by iceberg;

insert into tbl_ice_puffin select * from src_ice;

select count(*) from tbl_ice_puffin;
explain select min(a), count(distinct b), max(c) from tbl_ice_puffin;
desc formatted tbl_ice_puffin A;


-- Test hive.iceberg.stats.source = metastore
set hive.iceberg.stats.source=metastore;

drop table if exists tbl_ice;
create external table tbl_ice(
    a int,
    b string,
    c int)
stored by iceberg;

insert into tbl_ice select * from src_ice;

select count(*) from tbl_ice;
explain select min(a), count(distinct b), max(c) from tbl_ice;
desc formatted tbl_ice A;
