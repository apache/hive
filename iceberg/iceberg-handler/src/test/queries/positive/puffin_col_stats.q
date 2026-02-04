-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
set hive.fetch.task.conversion=none;

set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

set hive.iceberg.stats.source=iceberg;

drop table if exists tbl_ice_puffin;
create external table tbl_ice_puffin(
    a int,
    b string,
    c int)
stored by iceberg;

insert into tbl_ice_puffin values
    (1, 'one', 50),
    (2, 'two', 51),
    (2, 'two', 51),
    (2, 'two', 51),
    (3, 'three', 52),
    (4, 'four', 53),
    (5, 'five', 54),
    (111, 'one', 55),
    (333, 'two', 56);

explain select * from tbl_ice_puffin;
desc formatted tbl_ice_puffin a;
desc formatted tbl_ice_puffin b;
desc formatted tbl_ice_puffin c;

insert into tbl_ice_puffin values (1000, 'six', 1000), (5000, 'two', 5000);

explain select * from tbl_ice_puffin;
desc formatted tbl_ice_puffin a;
desc formatted tbl_ice_puffin b;
desc formatted tbl_ice_puffin c;

insert into tbl_ice_puffin values (10, 'six', 100000), (5000, 'two', 510000);

explain select * from tbl_ice_puffin;
desc formatted tbl_ice_puffin a;
desc formatted tbl_ice_puffin b;
desc formatted tbl_ice_puffin c;

-- Result:  a = (min: 1, max: 5000) , c =(min: 50, max: 51000)