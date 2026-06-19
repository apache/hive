--! qt:dataset:src
set hive.merge.mapredfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.tezfiles=true;
set hive.optimize.sort.dynamic.partition.threshold=-1;
set mapred.reduce.tasks=5;
set hive.blobstore.supported.schemes=hdfs,file;

create table orc_source(key string) stored by iceberg stored as orc tblproperties('format-version'='2');
insert overwrite table orc_source select key from src;
insert into table orc_source select key from src;
insert into table orc_source select key from src;
insert into table orc_source select key from src;

select count(*) from orc_source;

select count(distinct(file_path)) from default.orc_source.files;

delete from orc_source where key in (select key from src);

select count(*) from orc_source;

-- Only 1 file corresponding to deletes must be generated.
select count(distinct(file_path)) from default.orc_source.files;

-- Parquet file format
create table parquet_source(key string) stored by iceberg stored as parquet tblproperties('format-version'='2');
insert overwrite table parquet_source select key from src;
insert into table parquet_source select key from src;
insert into table parquet_source select key from src;
insert into table parquet_source select key from src;

select count(*) from parquet_source;

select count(distinct(file_path)) from default.parquet_source.files;

delete from parquet_source where key in (select key from src);

-- Only 1 file corresponding to deletes must be generated.
select count(distinct(file_path)) from default.parquet_source.files;

select count(*) from parquet_source;

-- Avro file format
create table avro_source(key string) stored by iceberg stored as avro tblproperties('format-version'='2');
insert overwrite table avro_source select key from src;
insert into table avro_source select key from src;
insert into table avro_source select key from src;
insert into table avro_source select key from src;

select count(*) from avro_source;

select count(distinct(file_path)) from default.avro_source.files;

delete from avro_source where key in (select key from src);

-- Only 1 file corresponding to deletes must be generated.
select count(distinct(file_path)) from default.avro_source.files;

select count(*) from avro_source;

create table orc_part_source(key string) partitioned by spec(key) stored by iceberg stored as orc tblproperties('format-version'='2');
insert overwrite table orc_part_source select key from src order by key limit 100;
insert into table orc_part_source select key from src order by key limit 100;
insert into table orc_part_source select key from src order by key limit 100;
insert into table orc_part_source select key from src order by key limit 100;

select count(*) from orc_part_source;

select count(distinct(file_path)) from default.orc_part_source.files;

delete from orc_part_source where key in (select key from src);

select count(*) from orc_part_source;

select count(distinct(file_path)) from default.orc_part_source.files;
