-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

drop table if exists tbl_ice;
set hive.iceberg.stats.source=metastore;
create external table tbl_ice(a int, b string, c int) stored by iceberg tblproperties ('format-version'='2');
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
explain select * from tbl_ice order by a, b, c;

drop table if exists tbl_ice;
set hive.iceberg.stats.source = iceberg;
create external table tbl_ice(a int, b string, c int) stored by iceberg tblproperties ('format-version'='2');
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
explain select * from tbl_ice order by a, b, c;

drop table if exists tbl_ice;
drop table if exists  t1 ;
drop table if exists  t2 ;
create table t1 (a int) stored by iceberg tblproperties ('format-version'='2');
create table t2 (b int) stored by iceberg tblproperties ('format-version'='2');
describe formatted t1;
describe formatted t2;
explain select * from t1 join t2 on t1.a = t2.b;

drop table if exists tbl_ice;
create external table tbl_ice(a int, b string, c int) stored by iceberg tblproperties ('format-version'='2');
explain select * from tbl_ice order by a, b, c;
select count(*) from tbl_ice ;
insert into tbl_ice values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);

explain select * from tbl_ice order by a, b, c;
select * from tbl_ice order by a, b, c;
select count(*) from tbl_ice ;

insert into tbl_ice values (1, 'one', 50), (2, 'two', 51),(2, 'two', 51),(2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);
explain select * from tbl_ice order by a, b, c;
select count(*) from tbl_ice ;
