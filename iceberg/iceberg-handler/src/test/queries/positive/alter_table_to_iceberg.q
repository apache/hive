-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/

set hive.vectorized.execution.enabled=false;

drop table if exists tbl_orc;
create external table tbl_orc(a int, b string) stored as orc;
describe formatted tbl_orc;
insert into table tbl_orc values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
select * from tbl_orc order by a;
explain alter table tbl_orc convert to iceberg;
alter table tbl_orc convert to iceberg;
describe formatted tbl_orc;
select * from tbl_orc order by a;
drop table tbl_orc;

drop table if exists tbl_parquet;
create external table tbl_parquet(a int, b string) stored as parquet;
describe formatted tbl_parquet;
insert into table tbl_parquet values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
select * from tbl_parquet order by a;
explain alter table tbl_parquet convert to iceberg;
alter table tbl_parquet convert to iceberg;
describe formatted tbl_parquet;
select * from tbl_parquet order by a;
drop table tbl_parquet;

drop table if exists tbl_avro;
create external table tbl_avro(a int, b string) stored as avro;
describe formatted tbl_avro;
insert into table tbl_avro values (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
select * from tbl_avro order by a;
explain alter table tbl_avro convert to iceberg;
alter table tbl_avro convert to iceberg;
describe formatted tbl_avro;
select * from tbl_avro order by a;
drop table tbl_avro;


set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists part_tbl_parquet;
create external table part_tbl_parquet (a int) partitioned by (s string) stored as parquet;
insert into part_tbl_parquet partition (s) values (1, '2023/05/18');
select * from part_tbl_parquet;

alter table part_tbl_parquet convert to iceberg;
select * from part_tbl_parquet;
drop table part_tbl_parquet;