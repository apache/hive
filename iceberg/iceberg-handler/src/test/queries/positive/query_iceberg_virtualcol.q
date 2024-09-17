drop table if exists tbl_ice;
create external table tbl_ice(a int, b string) partitioned by (c int) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into tbl_ice values (1, 'one', 50), (2, 'two', 50), (3, 'three', 50), (4, 'four', 52), (5, 'five', 54), (111, 'one', 52), (333, 'two', 56);

select tbl_ice.ROW__POSITION from tbl_ice;

select a, c, tbl_ice.PARTITION__SPEC__ID, tbl_ice.PARTITION__HASH, tbl_ice.ROW__POSITION from tbl_ice
order by tbl_ice.PARTITION__HASH, tbl_ice.ROW__POSITION desc;

select a, c, tbl_ice.PARTITION__SPEC__ID, tbl_ice.PARTITION__HASH, tbl_ice.ROW__POSITION from tbl_ice
sort by tbl_ice.PARTITION__HASH, tbl_ice.ROW__POSITION desc;


-- create a table with more than 4 columns
create table ice01 (a int, b int, c int, d int, e int) stored by iceberg tblproperties ('format-version'='2');
insert into ice01 values (1,2,3,4,5), (6,7,8,9,10);

select ice01.ROW__POSITION from ice01;