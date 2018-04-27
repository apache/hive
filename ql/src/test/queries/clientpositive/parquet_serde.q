--! qt:dataset:part

set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;

DROP TABLE if exists parquet_mixed_fileformat;

CREATE TABLE parquet_mixed_fileformat (
    id int,
    str string,
    part string
) PARTITIONED BY (dateint int)
 ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

---- partition dateint=20140330 is stored as TEXTFILE

LOAD DATA LOCAL INPATH '../../data/files/parquet_partitioned.txt' OVERWRITE INTO TABLE parquet_mixed_fileformat PARTITION (dateint=20140330);

SELECT * FROM parquet_mixed_fileformat;

DESCRIBE FORMATTED parquet_mixed_fileformat PARTITION (dateint=20140330);

---change table serde and file format to PARQUET----

ALTER TABLE parquet_mixed_fileformat set SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe';
ALTER TABLE parquet_mixed_fileformat
     SET FILEFORMAT
     INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
     OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe';

DESCRIBE FORMATTED parquet_mixed_fileformat;
DESCRIBE FORMATTED parquet_mixed_fileformat PARTITION (dateint=20140330);

SELECT * FROM parquet_mixed_fileformat;
