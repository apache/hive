DROP TABLE parquet_array_null_element_staging;
DROP TABLE parquet_array_null_element;

CREATE TABLE parquet_array_null_element_staging (
    id int,
    lstint ARRAY<INT>,
    lststr ARRAY<STRING>,
    mp  MAP<STRING,STRING>
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
NULL DEFINED AS '';

CREATE TABLE parquet_array_null_element (
    id int,
    lstint ARRAY<INT>,
    lststr ARRAY<STRING>,
    mp  MAP<STRING,STRING>
) STORED AS PARQUET;

DESCRIBE FORMATTED parquet_array_null_element;

LOAD DATA LOCAL INPATH '../../data/files/parquet_array_null_element.txt' OVERWRITE INTO TABLE parquet_array_null_element_staging;

SELECT * FROM parquet_array_null_element_staging;

INSERT OVERWRITE TABLE parquet_array_null_element SELECT * FROM parquet_array_null_element_staging;

SELECT lstint from parquet_array_null_element;
SELECT lststr from parquet_array_null_element;
SELECT mp from parquet_array_null_element;
SELECT * FROM parquet_array_null_element;
