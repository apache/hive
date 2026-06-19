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
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/

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
insert into table tbl_parquet values (10, '');
insert into table tbl_parquet values (11, null);
insert into table tbl_parquet values (12, NULL);
select count(*) from tbl_parquet;
select * from tbl_parquet order by a;
explain alter table tbl_parquet convert to iceberg;
alter table tbl_parquet convert to iceberg;
describe formatted tbl_parquet;
select count(*) from tbl_parquet;
select * from tbl_parquet order by a;
insert into table tbl_parquet partition (b='five') values (13);
insert into table tbl_parquet values (14, '');
insert into table tbl_parquet values (15, null);
insert into table tbl_parquet values (16, NULL);
select count(*) from tbl_parquet;
select * from tbl_parquet order by a;
drop table tbl_parquet;

drop table if exists tbl_parquet_int;
create external table tbl_parquet_int(a int) partitioned by (b int) stored as parquet;
describe formatted tbl_parquet_int;
insert into table tbl_parquet_int partition (b=1) values (1), (2), (3);
insert into table tbl_parquet_int partition (b=2) values (4), (5);
insert into table tbl_parquet_int partition (b=3) values (6), (7), (8);
insert into table tbl_parquet_int partition (b=4) values (9);
insert into table tbl_parquet_int values (10, '');
insert into table tbl_parquet_int values (11, null);
insert into table tbl_parquet_int values (12, NULL);
select count(*) from tbl_parquet_int;
select * from tbl_parquet_int order by a;
explain alter table tbl_parquet_int convert to iceberg;
alter table tbl_parquet_int convert to iceberg;
describe formatted tbl_parquet_int;
select count(*) from tbl_parquet_int;
select * from tbl_parquet_int order by a;
insert into table tbl_parquet_int partition (b=5) values (13);
insert into table tbl_parquet_int values (14, '');
insert into table tbl_parquet_int values (15, null);
insert into table tbl_parquet_int values (16, NULL);
select count(*) from tbl_parquet_int;
select * from tbl_parquet_int order by a;
drop table tbl_parquet_int;

drop table if exists tbl_parquet_double;
create external table tbl_parquet_double(a int) partitioned by (b double) stored as parquet;
describe formatted tbl_parquet_double;
insert into table tbl_parquet_double partition (b=1.1) values (1), (2), (3);
insert into table tbl_parquet_double partition (b=2.22) values (4), (5);
insert into table tbl_parquet_double partition (b=3.333) values (6), (7), (8);
insert into table tbl_parquet_double partition (b=4.4444) values (9);
insert into table tbl_parquet_double values (10, '');
insert into table tbl_parquet_double values (11, null);
insert into table tbl_parquet_double values (12, NULL);
select count(*) from tbl_parquet_double;
select * from tbl_parquet_double order by a;
explain alter table tbl_parquet_double convert to iceberg;
alter table tbl_parquet_double convert to iceberg;
describe formatted tbl_parquet_double;
select count(*) from tbl_parquet_double;
select * from tbl_parquet_double order by a;
insert into table tbl_parquet_double partition (b=5.55555) values (13);
insert into table tbl_parquet_double values (14, '');
insert into table tbl_parquet_double values (15, null);
insert into table tbl_parquet_double values (16, NULL);
select count(*) from tbl_parquet_double;
select * from tbl_parquet_double order by a;
drop table tbl_parquet_double;

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