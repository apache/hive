set hive.vectorized.execution.enabled=false;

-- this test creates a Parquet table with an array of multi-field structs

CREATE TABLE parquet_array_of_multi_field_structs (
    locations ARRAY<STRUCT<latitude: DOUBLE, longitude: DOUBLE>>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/MultiFieldGroupInList.parquet'
OVERWRITE INTO TABLE parquet_array_of_multi_field_structs;

SELECT * FROM parquet_array_of_multi_field_structs;

DROP TABLE parquet_array_of_multi_field_structs;

-- maps use the same writable structure, so validate that the data can be read
-- as a map instead of an array of structs

CREATE TABLE parquet_map_view_of_multi_field_structs (
    locations MAP<DOUBLE, DOUBLE>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/MultiFieldGroupInList.parquet'
OVERWRITE INTO TABLE parquet_map_view_of_multi_field_structs;

SELECT * FROM parquet_map_view_of_multi_field_structs;

DROP TABLE parquet_map_view_of_multi_field_structs;
