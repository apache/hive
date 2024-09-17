create external table ice_parquet(
  strcol string,
  intcol integer
) partitioned by (pcol int)
stored by iceberg;

explain LOAD DATA LOCAL INPATH '../../data/files/parquet_partition' OVERWRITE INTO TABLE ice_parquet;
explain analyze LOAD DATA LOCAL INPATH '../../data/files/parquet_partition' OVERWRITE INTO TABLE ice_parquet;

LOAD DATA LOCAL INPATH '../../data/files/parquet_partition' OVERWRITE INTO TABLE ice_parquet;

select * from ice_parquet order by intcol;

CREATE TABLE ice_avro (
  number int,
  first_name string)
stored by iceberg
STORED AS AVRO;

explain LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' OVERWRITE INTO TABLE ice_avro;
explain analyze LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' OVERWRITE INTO TABLE ice_avro;

set hive.load.data.use.native.api=false;

explain LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' OVERWRITE INTO TABLE ice_avro;

set hive.load.data.use.native.api=true;

LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' OVERWRITE INTO TABLE ice_avro;

select * from ice_avro order by number;

CREATE TABLE ice_orc (
  p_partkey int,
  p_name string,
  p_mfgr string,
  p_brand string,
  p_type string,
  p_size int,
  p_container string,
  p_retailprice double,
  p_comment string
)
stored by iceberg
STORED AS ORC;

explain LOAD DATA LOCAL INPATH '../../data/files/part.orc' OVERWRITE INTO TABLE ice_orc;
explain analyze LOAD DATA LOCAL INPATH '../../data/files/part.orc' OVERWRITE INTO TABLE ice_orc;

LOAD DATA LOCAL INPATH '../../data/files/part.orc' OVERWRITE INTO TABLE ice_orc;

select * from ice_orc order by p_partkey;

select count(*) from ice_orc;

LOAD DATA LOCAL INPATH '../../data/files/part.orc' INTO TABLE ice_orc;

select * from ice_orc order by p_partkey;

select count(*) from ice_orc;

create external table ice_parquet_partitioned (
  strcol string,
  intcol integer
) partitioned by (pcol int)
stored by iceberg;

insert into ice_parquet_partitioned values ('AA', 10, 100), ('BB', 20, 200), ('CC', 30, 300);

select * from ice_parquet_partitioned order by intcol;

explain LOAD DATA LOCAL INPATH '../../data/files/parquet_partition/pcol=100' INTO TABLE ice_parquet_partitioned
PARTITION (pcol='300');

LOAD DATA LOCAL INPATH '../../data/files/parquet_partition/pcol=100' INTO TABLE
ice_parquet_partitioned PARTITION (pcol='100');

select * from ice_parquet_partitioned order by intcol;

explain LOAD DATA LOCAL INPATH '../../data/files/parquet_partition/pcol=200' OVERWRITE INTO TABLE
        ice_parquet_partitioned PARTITION (pcol='200');

LOAD DATA LOCAL INPATH '../../data/files/parquet_partition/pcol=200' OVERWRITE INTO TABLE
ice_parquet_partitioned PARTITION (pcol='200');

select * from ice_parquet_partitioned order by intcol;