CREATE EXTERNAL TABLE hive_28518_test(`id` int,`name` string,`dt` timestamp) STORED AS PARQUET;
insert into hive_28518_test values (1, "test name" , cast('2024-08-09 14:08:26.326107' as timestamp));
ALTER TABLE hive_28518_test SET TBLPROPERTIES ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler', 'format-version' = '2');
set hive.fetch.task.conversion=more;
select * from hive_28518_test;
set hive.fetch.task.conversion=none;
select * from hive_28518_test;