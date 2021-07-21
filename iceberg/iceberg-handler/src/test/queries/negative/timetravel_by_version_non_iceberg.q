create table tbl_orc (a int, b string);
select * from tbl_orc FOR SYSTEM_VERSION AS OF 12345;