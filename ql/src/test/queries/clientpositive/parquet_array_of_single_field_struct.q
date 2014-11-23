-- this test creates a Parquet table with an array of single-field structs
-- that has an ambiguous Parquet schema that is assumed to be a list of bigints
-- This is verifies compliance with the spec for this case.

CREATE TABLE parquet_ambiguous_array_of_single_field_structs (
    single_element_groups ARRAY<BIGINT>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/SingleFieldGroupInList.parquet'
OVERWRITE INTO TABLE parquet_ambiguous_array_of_single_field_structs;

SELECT * FROM parquet_ambiguous_array_of_single_field_structs;

DROP TABLE parquet_ambiguous_array_of_single_field_structs;
