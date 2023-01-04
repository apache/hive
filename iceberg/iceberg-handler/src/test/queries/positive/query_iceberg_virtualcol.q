drop table if exists tbl_ice;
create external table tbl_ice(a int, b string) partitioned by (c int) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into tbl_ice values (1, 'one', 50), (2, 'two', 50), (3, 'three', 50), (4, 'four', 52), (5, 'five', 54), (111, 'one', 52), (333, 'two', 56);

select a, c, tbl_ice.PARTITION__SPEC__ID, tbl_ice.PARTITION__HASH, tbl_ice.ROW__POSITION from tbl_ice
order by tbl_ice.PARTITION__HASH, tbl_ice.ROW__POSITION desc;

select a, c, tbl_ice.PARTITION__SPEC__ID, tbl_ice.PARTITION__HASH, tbl_ice.ROW__POSITION from tbl_ice
sort by tbl_ice.PARTITION__HASH, tbl_ice.ROW__POSITION desc;
