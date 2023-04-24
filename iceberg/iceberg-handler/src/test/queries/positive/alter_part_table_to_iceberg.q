-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+/$1#Masked#/
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
create external table tbl_orc(a int) partitioned by (b string) stored as orc;
describe formatted tbl_orc;
insert into table tbl_orc partition (b='one') values (1), (2), (3);
insert into table tbl_orc partition (b='two') values (4), (5);
insert into table tbl_orc partition (b='three') values (6), (7), (8);
insert into table tbl_orc partition (b='four') values (9);
select * from tbl_orc order by a;
explain alter table tbl_orc convert to iceberg;
alter table tbl_orc convert to iceberg;
describe formatted tbl_orc;
select * from tbl_orc order by a;
drop table tbl_orc;

drop table if exists tbl_parquet;
create external table tbl_parquet(a int) partitioned by (b string) stored as parquet;
describe formatted tbl_parquet;
insert into table tbl_parquet partition (b='one') values (1), (2), (3);
insert into table tbl_parquet partition (b='two') values (4), (5);
insert into table tbl_parquet partition (b='three') values (6), (7), (8);
insert into table tbl_parquet partition (b='four') values (9);
select * from tbl_parquet order by a;
explain alter table tbl_parquet convert to iceberg;
alter table tbl_parquet convert to iceberg;
describe formatted tbl_parquet;
select * from tbl_parquet order by a;
drop table tbl_parquet;

drop table if exists tbl_avro;
create external table tbl_avro(a int) partitioned by (b string) stored as avro;
describe formatted tbl_avro;
insert into table tbl_avro partition (b='one') values (1), (2), (3);
insert into table tbl_avro partition (b='two') values (4), (5);
insert into table tbl_avro partition (b='three') values (6), (7), (8);
insert into table tbl_avro partition (b='four') values (9);
select * from tbl_avro order by a;
explain alter table tbl_avro convert to iceberg;
alter table tbl_avro convert to iceberg;
describe formatted tbl_avro;
select * from tbl_avro order by a;
drop table tbl_avro;