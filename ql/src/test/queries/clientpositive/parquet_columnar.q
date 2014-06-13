set parquet.column.index.access=true;

DROP TABLE IF EXISTS parquet_columnar_access_stage;
DROP TABLE IF EXISTS parquet_columnar_access;
DROP TABLE IF EXISTS parquet_columnar_renamed;

CREATE TABLE parquet_columnar_access_stage (
    s string, 
    i int, 
    f float
  ) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|';

CREATE TABLE parquet_columnar_access (
    s string,
    i int,
    f float
  ) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/parquet_columnar.txt' OVERWRITE INTO TABLE parquet_columnar_access_stage;

INSERT OVERWRITE TABLE parquet_columnar_access SELECT * FROM parquet_columnar_access_stage;
SELECT * FROM parquet_columnar_access;

ALTER TABLE parquet_columnar_access REPLACE COLUMNS (s1 string, i1 int, f1 float);

SELECT * FROM parquet_columnar_access;
