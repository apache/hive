-- this test creates a Parquet table with an array of structs

CREATE TABLE parquet_avro_array_of_primitives (
    list_of_ints ARRAY<INT>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/AvroPrimitiveInList.parquet'
OVERWRITE INTO TABLE parquet_avro_array_of_primitives;

SELECT * FROM parquet_avro_array_of_primitives;

DROP TABLE parquet_avro_array_of_primitives;
