set hive.mapred.mode=nonstrict;
DROP TABLE if exists parquet_mixed_partition_formats;

CREATE TABLE parquet_mixed_partition_formats (
  cint int,
  ctinyint tinyint,
  csmallint smallint,
  cfloat float,
  cdouble double,
  cstring1 string,
  t timestamp,
  cchar char(5),
  cvarchar varchar(10),
  cbinary string,
  m1 map<string, varchar(3)>,
  l1 array<int>,
  st1 struct<c1:int, c2:char(1)>,
  d date)
PARTITIONED BY (dateint int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

---- partition dateint=20140330 is stored as TEXTFILE
LOAD DATA LOCAL INPATH '../../data/files/parquet_types.txt' OVERWRITE INTO TABLE parquet_mixed_partition_formats PARTITION (dateint=20140330);

SELECT * FROM parquet_mixed_partition_formats;

DESCRIBE FORMATTED parquet_mixed_partition_formats PARTITION (dateint=20140330);

---change table serde and file format to PARQUET----

ALTER TABLE parquet_mixed_partition_formats
     SET FILEFORMAT
     INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
     OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
     SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe';

DESCRIBE FORMATTED parquet_mixed_partition_formats;
DESCRIBE FORMATTED parquet_mixed_partition_formats PARTITION (dateint=20140330);

SELECT * FROM parquet_mixed_partition_formats;
