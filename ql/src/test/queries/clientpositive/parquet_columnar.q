set hive.vectorized.execution.enabled=false;
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
    x int,
    y int,
    f float,
    address struct<intVals:int,strVals:string>
  ) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/parquet_columnar.txt' OVERWRITE INTO TABLE parquet_columnar_access_stage;

INSERT OVERWRITE TABLE parquet_columnar_access SELECT s, i, (i + 1), f, named_struct('intVals',
i,'strVals',s) FROM parquet_columnar_access_stage;
SELECT * FROM parquet_columnar_access;

ALTER TABLE parquet_columnar_access REPLACE COLUMNS (s1 string, x1 int, y1 int, f1 float);

SELECT * FROM parquet_columnar_access;

ALTER TABLE parquet_columnar_access REPLACE COLUMNS (s1 string, x1 bigint, y1 int, f1 double);

SELECT * FROM parquet_columnar_access;

ALTER TABLE parquet_columnar_access REPLACE COLUMNS (s1 string, x1 float, y1 float, f1 double);

SELECT * FROM parquet_columnar_access;