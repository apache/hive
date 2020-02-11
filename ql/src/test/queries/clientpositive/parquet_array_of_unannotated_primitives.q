set hive.vectorized.execution.enabled=false;

-- this test creates a Parquet table from a structure with an unannotated
-- repeated structure of int32s

CREATE TABLE parquet_array_of_unannotated_ints (
    list_of_ints ARRAY<INT>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/UnannotatedListOfPrimitives.parquet'
OVERWRITE INTO TABLE parquet_array_of_unannotated_ints;

SELECT * FROM parquet_array_of_unannotated_ints;

DROP TABLE parquet_array_of_unannotated_ints;
