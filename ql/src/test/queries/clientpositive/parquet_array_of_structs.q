set hive.vectorized.execution.enabled=false;

-- this test creates a Parquet table with an array of structs

CREATE TABLE parquet_array_of_structs (
    locations ARRAY<STRUCT<latitude: DOUBLE, longitude: DOUBLE>>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/HiveRequiredGroupInList.parquet'
OVERWRITE INTO TABLE parquet_array_of_structs;

SELECT * FROM parquet_array_of_structs;

DROP TABLE parquet_array_of_structs;
