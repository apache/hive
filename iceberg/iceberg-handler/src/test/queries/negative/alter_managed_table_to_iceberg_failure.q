drop table if exists tbl_orc;
create table tbl_orc(a int, b string) stored as orc;
alter table tbl_orc set tblproperties ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler');