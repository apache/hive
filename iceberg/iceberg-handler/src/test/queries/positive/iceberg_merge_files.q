--! qt:dataset:src
set hive.merge.mapredfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.tezfiles=true;
set hive.optimize.sort.dynamic.partition.threshold=-1;
set mapred.reduce.tasks=5;
set hive.blobstore.supported.schemes=hdfs,file;

-- SORT_QUERY_RESULTS
create table orc_part_source(key string, value string, ds string) partitioned by spec (ds) stored by iceberg stored as orc;
create table orc_source(key string) stored by iceberg stored as orc;

-- The partitioned table must have 2 files per partition (necessary for merge task)
insert overwrite table orc_part_source partition(ds='102') select * from src;
insert into table orc_part_source partition(ds='102') select * from src;
insert overwrite table orc_part_source partition(ds='103') select * from src;
insert into table orc_part_source partition(ds='103') select * from src;

-- The unpartitioned table must have 2 files.
insert overwrite table orc_source select key from src;
insert into table orc_source select key from src;

select count(*) from orc_source;
select count(*) from orc_part_source;

select count(distinct(file_path)) from default.orc_source.files;
select count(distinct(file_path)) from default.orc_part_source.files;

-- Insert into the tables both for unpartitioned and partitioned cases for ORC formats.
insert into table orc_source select * from orc_source;
insert into table orc_part_source select * from orc_part_source where ds = 102 union all select * from orc_part_source where ds = 103;

select count(*) from orc_source;
select count(*) from orc_part_source;

select count(distinct(file_path)) from default.orc_source.files;
select count(distinct(file_path)) from default.orc_part_source.files;

create table parquet_part_source(key string, value string, ds string) partitioned by spec (ds) stored by iceberg stored as parquet;
create table parquet_source(key string) stored by iceberg stored as parquet;

-- The partitioned table must have 2 files per partition (necessary for merge task)
insert overwrite table parquet_part_source partition(ds='102') select * from src;
insert into table parquet_part_source partition(ds='102') select * from src;
insert overwrite table parquet_part_source partition(ds='103') select * from src;
insert into table parquet_part_source partition(ds='103') select * from src;

-- The unpartitioned table must have 2 files.
insert overwrite table parquet_source select key from src;
insert into table parquet_source select key from src;

select count(*) from parquet_source;
select count(*) from parquet_part_source;

select count(distinct(file_path)) from default.parquet_source.files;
select count(distinct(file_path)) from default.parquet_part_source.files;

-- Insert into the tables both for unpartitioned and partitioned cases for Parquet formats.
insert into table parquet_source select * from parquet_source;
insert into table parquet_part_source select * from parquet_part_source where ds = 102 union all select * from parquet_part_source where ds = 103;

select count(*) from parquet_source;
select count(*) from parquet_part_source;

select count(distinct(file_path)) from default.parquet_source.files;
select count(distinct(file_path)) from default.parquet_part_source.files;

create table avro_part_source(key string, value string, ds string) partitioned by spec (ds) stored by iceberg stored as avro;
create table avro_source(key string) stored by iceberg stored as avro;

-- The partitioned table must have 2 files per partition (necessary for merge task)
insert overwrite table avro_part_source partition(ds='102') select * from src;
insert into table avro_part_source partition(ds='102') select * from src;
insert overwrite table avro_part_source partition(ds='103') select * from src;
insert into table avro_part_source partition(ds='103') select * from src;

-- The unpartitioned table must have 2 files.
insert overwrite table avro_source select key from src;
insert into table avro_source select key from src;

select count(*) from avro_source;
select count(*) from avro_part_source;

select count(distinct(file_path)) from default.avro_source.files;
select count(distinct(file_path)) from default.avro_part_source.files;

-- Insert into the tables both for unpartitioned and partitioned cases for Avro formats.
insert into table avro_source select * from avro_source;
insert into table avro_part_source select * from avro_part_source where ds = 102 union all select * from avro_part_source where ds = 103;

select count(*) from avro_source;
select count(*) from avro_part_source;

select count(distinct(file_path)) from default.avro_source.files;
select count(distinct(file_path)) from default.avro_part_source.files;

-- Generate 5 files in the CTAS source table.
create table ctas_source stored by iceberg stored as orc as select * from src;
insert into table ctas_source select * from src;
insert into table ctas_source select * from src;
insert into table ctas_source select * from src;
insert into table ctas_source select * from src;
select count(*) from ctas_source;

-- Check whether a single file is created post merge task for CTAS and CMV.
create table ctas_orc_table stored by iceberg stored as orc as select * from ctas_source;
select count(distinct(file_path)) from default.ctas_orc_table.files;
select count(*) from ctas_orc_table;

create materialized view cmv_orc stored by iceberg stored as orc as select * from ctas_source;
select count(distinct(file_path)) from default.cmv_orc.files;
select count(*) from cmv_orc;

create table ctas_parquet_table stored by iceberg stored as parquet as select * from ctas_source;
select count(distinct(file_path)) from default.ctas_parquet_table.files;
select count(*) from ctas_parquet_table;

create materialized view cmv_parquet stored by iceberg stored as parquet as select * from ctas_source;
select count(distinct(file_path)) from default.cmv_parquet.files;
select count(*) from cmv_parquet;

create table ctas_avro_table stored by iceberg stored as avro as select * from ctas_source;
select count(distinct(file_path)) from default.ctas_avro_table.files;
select count(*) from ctas_avro_table;

create materialized view cmv_avro stored by iceberg stored as avro as select * from ctas_source;
select count(distinct(file_path)) from default.cmv_avro.files;
select count(*) from cmv_avro;
