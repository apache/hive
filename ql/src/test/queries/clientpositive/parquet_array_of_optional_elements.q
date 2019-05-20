set hive.vectorized.execution.enabled=false;

-- this test creates a Parquet table with an array of optional structs

CREATE TABLE parquet_array_of_optional_elements (
    locations ARRAY<STRUCT<latitude: DOUBLE, longitude: DOUBLE>>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/NewOptionalGroupInList.parquet'
OVERWRITE INTO TABLE parquet_array_of_optional_elements;

SELECT * FROM parquet_array_of_optional_elements;

DROP TABLE parquet_array_of_optional_elements;
