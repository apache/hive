set hive.vectorized.execution.enabled=false;

-- this test creates a Parquet table with an array of single-field structs
-- as written by parquet-thrift

CREATE TABLE parquet_thrift_array_of_single_field_structs (
    single_element_groups ARRAY<STRUCT<count: BIGINT>>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/ThriftSingleFieldGroupInList.parquet'
OVERWRITE INTO TABLE parquet_thrift_array_of_single_field_structs;

SELECT * FROM parquet_thrift_array_of_single_field_structs;

DROP TABLE parquet_thrift_array_of_single_field_structs;
