set hive.vectorized.execution.enabled=false;

drop table if exists tbl_orc;
create external table tbl_orc(a int, b string) stored as orc;
describe formatted tbl_orc;
insert into table tbl_orc values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
select * from tbl_orc order by a;
alter table tbl_orc set tblproperties ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler');
describe formatted tbl_orc;
select * from tbl_orc order by a;
drop table tbl_orc;

drop table if exists tbl_parquet;
create external table tbl_parquet(a int, b string) stored as parquet;
describe formatted tbl_parquet;
insert into table tbl_parquet values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
select * from tbl_parquet order by a;
alter table tbl_parquet set tblproperties ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler');
describe formatted tbl_parquet;
select * from tbl_parquet order by a;
drop table tbl_parquet;

drop table if exists tbl_avro;
create external table tbl_avro(a int, b string) stored as avro;
describe formatted tbl_avro;
insert into table tbl_avro values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
select * from tbl_avro order by a;
alter table tbl_avro set tblproperties ('storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler');
describe formatted tbl_avro;
select * from tbl_avro order by a;
drop table tbl_avro;