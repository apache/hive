-- This test attempts to write a parquet table from an avro table that contains map null values

DROP TABLE IF EXISTS avro_table;
DROP TABLE IF EXISTS parquet_table;

CREATE TABLE avro_table (avreau_col_1 map<string,string>) STORED AS AVRO;
LOAD DATA LOCAL INPATH '../../data/files/map_null_val.avro' OVERWRITE INTO TABLE avro_table;

CREATE TABLE parquet_table STORED AS PARQUET AS SELECT * FROM avro_table;
SELECT * FROM parquet_table;

DROP TABLE avro_table;
DROP TABLE parquet_table;
