-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

drop table if exists tbl_ice;
create external table tbl_ice(a int, b string, c int) stored by iceberg tblproperties ('format-version'='2');
select count(*) from tbl_ice ;
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);

explain select * from tbl_ice order by a, b, c;
select * from tbl_ice order by a, b, c;
select count(*) from tbl_ice ;

insert into tbl_ice values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
explain select * from tbl_ice order by a, b, c;
select count(*) from tbl_ice ;

drop table if exists tbl_orc;
create external table tbl_orc(a int, b string, c int) stored as orc tblproperties ('format-version'='2');
insert into tbl_orc values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four',
53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
select count(*) from tbl_orc ;