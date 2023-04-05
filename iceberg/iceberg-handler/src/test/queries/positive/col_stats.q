-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

set hive.iceberg.stats.source=iceberg;
drop table if exists tbl_ice_puffin;
create external table tbl_ice_puffin(a int, b string, c int) stored by iceberg tblproperties ('format-version'='2');
insert into tbl_ice_puffin values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
insert into tbl_ice_puffin values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
explain select * from tbl_ice_puffin order by a, b, c;
select * from tbl_ice_puffin order by a, b, c;
desc formatted tbl_ice_puffin b;
update tbl_ice_puffin set b='two' where b='one' or b='three';
analyze table tbl_ice_puffin  compute statistics for columns;
explain select * from tbl_ice_puffin order by a, b, c;
select * from tbl_ice_puffin order by a, b, c;
select count(*) from tbl_ice_puffin ;
desc formatted tbl_ice_puffin b;


-- Test if hive.iceberg.stats.source is empty
set hive.iceberg.stats.source= ;
drop table if exists tbl_ice_puffin;
create external table tbl_ice_puffin(a int, b string, c int) stored by iceberg tblproperties ('format-version'='2');
insert into tbl_ice_puffin values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
explain select * from tbl_ice_puffin order by a, b, c;


set hive.iceberg.stats.source=iceberg;
drop table if exists tbl_ice_puffin;
create external table tbl_ice_puffin(a int, b string, c int) stored by iceberg tblproperties ('format-version'='2');
insert into tbl_ice_puffin values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
explain select * from tbl_ice_puffin order by a, b, c;
select * from tbl_ice_puffin order by a, b, c;
select count(*) from tbl_ice_puffin ;
desc formatted tbl_ice_puffin a;


set hive.iceberg.stats.source=metastore;

drop table if exists tbl_ice;
create external table tbl_ice(a int, b string, c int) stored by iceberg tblproperties ('format-version'='2');
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
explain select * from tbl_ice order by a, b, c;
select * from tbl_ice order by a, b, c;
select count(*) from tbl_ice ;

set hive.iceberg.stats.source=iceberg;
delete from tbl_ice_puffin  where  a = 2;
explain select * from tbl_ice order by a, b, c;
select count(*) from tbl_ice ;

create table t1 (a int) stored by iceberg tblproperties ('format-version'='2');
create table t2 (b int) stored by iceberg tblproperties ('format-version'='2');
describe formatted t1;
describe formatted t2;
explain select * from t1 join t2 on t1.a = t2.b;