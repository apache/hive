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
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/

set hive.vectorized.execution.enabled=false;

drop table if exists tbl_orc;
create external table tbl_orc(a int) partitioned by (b string, c string) stored as orc;
describe formatted tbl_orc;
insert into table tbl_orc partition (b='one', c='Monday') values (1), (2), (3);
insert into table tbl_orc partition (b='two', c='Tuesday') values (4), (5);
insert into table tbl_orc partition (b='two', c='Friday') values (10), (11);
insert into table tbl_orc partition (b='three', c='Wednesday') values (6), (7), (8);
insert into table tbl_orc partition (b='four', c='Thursday') values (9);
insert into table tbl_orc partition (b='four', c='Saturday') values (12), (13), (14);
insert into table tbl_orc partition (b='four', c='Sunday') values (15);
select * from tbl_orc order by a;
explain alter table tbl_orc convert to iceberg;
alter table tbl_orc convert to iceberg;
describe formatted tbl_orc;
select * from tbl_orc order by a;
drop table tbl_orc;

drop table if exists tbl_parquet;
create external table tbl_parquet(a int) partitioned by (b string, c string) stored as parquet;
describe formatted tbl_parquet;
insert into table tbl_parquet partition (b='one', c='Monday') values (1), (2), (3);
insert into table tbl_parquet partition (b='two', c='Tuesday') values (4), (5);
insert into table tbl_parquet partition (b='two', c='Friday') values (10), (11);
insert into table tbl_parquet partition (b='three', c='Wednesday') values (6), (7), (8);
insert into table tbl_parquet partition (b='four', c='Thursday') values (9);
insert into table tbl_parquet partition (b='four', c='Saturday') values (12), (13), (14);
insert into table tbl_parquet partition (b='four', c='Sunday') values (15);
select * from tbl_parquet order by a;
explain alter table tbl_parquet convert to iceberg;
alter table tbl_parquet convert to iceberg;
describe formatted tbl_parquet;
select * from tbl_parquet order by a;
drop table tbl_parquet;

drop table if exists tbl_avro;
create external table tbl_avro(a int) partitioned by (b string, c string) stored as avro;
describe formatted tbl_avro;
insert into table tbl_avro partition (b='one', c='Monday') values (1), (2), (3);
insert into table tbl_avro partition (b='two', c='Tuesday') values (4), (5);
insert into table tbl_avro partition (b='two', c='Friday') values (10), (11);
insert into table tbl_avro partition (b='three', c='Wednesday') values (6), (7), (8);
insert into table tbl_avro partition (b='four', c='Thursday') values (9);
insert into table tbl_avro partition (b='four', c='Saturday') values (12), (13), (14);
insert into table tbl_avro partition (b='four', c='Sunday') values (15);
select * from tbl_avro order by a;
explain alter table tbl_avro convert to iceberg;
alter table tbl_avro convert to iceberg;
describe formatted tbl_avro;
select * from tbl_avro order by a;
drop table tbl_avro;

drop table if exists tbl_orc_mixed;
create external table tbl_orc_mixed(a int) partitioned by (b double, c int, d string) stored as orc;
describe formatted tbl_orc_mixed;
insert into table tbl_orc_mixed partition (b=1.1, c=2, d='one') values (1), (2), (3);
insert into table tbl_orc_mixed partition (b=2.22, c=1, d='two') values (4), (5);
insert into table tbl_orc_mixed partition (b=3.333, c=3, d='three') values (6), (7), (8);
insert into table tbl_orc_mixed partition (b=4.4444, c=4, d='four') values (9);
insert into table tbl_orc_mixed values (10, '', '', '');
insert into table tbl_orc_mixed values (11, null, null, null);
insert into table tbl_orc_mixed values (12, NULL, NULL, NULL);
insert into table tbl_orc_mixed values (13, '', -2, '');
insert into table tbl_orc_mixed values (14, null, null, 'random');
insert into table tbl_orc_mixed values (15, -0.11, NULL, NULL);
select count(*) from tbl_orc_mixed;
select * from tbl_orc_mixed order by a;
explain alter table tbl_orc_mixed convert to iceberg;
alter table tbl_orc_mixed convert to iceberg;
describe formatted tbl_orc_mixed;
select count(*) from tbl_orc_mixed;
select * from tbl_orc_mixed order by a;
insert into table tbl_orc_mixed partition (b=5.55555, c = 5, d = 'five') values (16);
insert into table tbl_orc_mixed values (17, '', '', '');
insert into table tbl_orc_mixed values (18, null, null, null);
insert into table tbl_orc_mixed values (19, NULL, NULL, NULL);
insert into table tbl_orc_mixed values (20, '', -3, '');
insert into table tbl_orc_mixed values (21, null, null, 'part');
insert into table tbl_orc_mixed values (22, -0.234, NULL, NULL);
select count(*) from tbl_orc_mixed;
select * from tbl_orc_mixed order by a;
drop table tbl_orc_mixed;

drop table if exists tbl_parquet_mixed;
create external table tbl_parquet_mixed(a int) partitioned by (b double, c int, d string) stored as parquet;
describe formatted tbl_parquet_mixed;
insert into table tbl_parquet_mixed partition (b=1.1, c=2, d='one') values (1), (2), (3);
insert into table tbl_parquet_mixed partition (b=2.22, c=1, d='two') values (4), (5);
insert into table tbl_parquet_mixed partition (b=3.333, c=3, d='three') values (6), (7), (8);
insert into table tbl_parquet_mixed partition (b=4.4444, c=4, d='four') values (9);
insert into table tbl_parquet_mixed values (10, '', '', '');
insert into table tbl_parquet_mixed values (11, null, null, null);
insert into table tbl_parquet_mixed values (12, NULL, NULL, NULL);
insert into table tbl_parquet_mixed values (13, '', -2, '');
insert into table tbl_parquet_mixed values (14, null, null, 'random');
insert into table tbl_parquet_mixed values (15, -0.11, NULL, NULL);
select count(*) from tbl_parquet_mixed;
select * from tbl_parquet_mixed order by a;
explain alter table tbl_parquet_mixed convert to iceberg;
alter table tbl_parquet_mixed convert to iceberg;
describe formatted tbl_parquet_mixed;
select count(*) from tbl_parquet_mixed;
select * from tbl_parquet_mixed order by a;
insert into table tbl_parquet_mixed partition (b=5.55555, c = 5, d = 'five') values (16);
insert into table tbl_parquet_mixed values (17, '', '', '');
insert into table tbl_parquet_mixed values (18, null, null, null);
insert into table tbl_parquet_mixed values (19, NULL, NULL, NULL);
insert into table tbl_parquet_mixed values (20, '', -3, '');
insert into table tbl_parquet_mixed values (21, null, null, 'part');
insert into table tbl_parquet_mixed values (22, -0.234, NULL, NULL);
select count(*) from tbl_parquet_mixed;
select * from tbl_parquet_mixed order by a;
drop table tbl_parquet_mixed;

drop table if exists tbl_avro_mixed;
create external table tbl_avro_mixed(a int) partitioned by (b double, c int, d string) stored as avro;
describe formatted tbl_avro_mixed;
insert into table tbl_avro_mixed partition (b=1.1, c=2, d='one') values (1), (2), (3);
insert into table tbl_avro_mixed partition (b=2.22, c=1, d='two') values (4), (5);
insert into table tbl_avro_mixed partition (b=3.333, c=3, d='three') values (6), (7), (8);
insert into table tbl_avro_mixed partition (b=4.4444, c=4, d='four') values (9);
insert into table tbl_avro_mixed values (10, '', '', '');
insert into table tbl_avro_mixed values (11, null, null, null);
insert into table tbl_avro_mixed values (12, NULL, NULL, NULL);
insert into table tbl_avro_mixed values (13, '', -2, '');
insert into table tbl_avro_mixed values (14, null, null, 'random');
insert into table tbl_avro_mixed values (15, -0.11, NULL, NULL);
select count(*) from tbl_avro_mixed;
select * from tbl_avro_mixed order by a;
explain alter table tbl_avro_mixed convert to iceberg;
alter table tbl_avro_mixed convert to iceberg;
describe formatted tbl_avro_mixed;
select count(*) from tbl_avro_mixed;
select * from tbl_avro_mixed order by a;
insert into table tbl_avro_mixed partition (b=5.55555, c = 5, d = 'five') values (16);
insert into table tbl_avro_mixed values (17, '', '', '');
insert into table tbl_avro_mixed values (18, null, null, null);
insert into table tbl_avro_mixed values (19, NULL, NULL, NULL);
insert into table tbl_avro_mixed values (20, '', -3, '');
insert into table tbl_avro_mixed values (21, null, null, 'part');
insert into table tbl_avro_mixed values (22, -0.234, NULL, NULL);
select count(*) from tbl_avro_mixed;
select * from tbl_avro_mixed order by a;
drop table tbl_avro_mixed;