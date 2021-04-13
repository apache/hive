drop table if exists tbl_orc;
create external table tbl_orc(a int, b string) partitioned by (c string) clustered by (b) into 2 buckets stored as orc;
alter table tbl_orc set tblproperties ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler');