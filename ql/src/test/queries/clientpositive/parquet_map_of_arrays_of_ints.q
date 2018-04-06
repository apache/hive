-- Suppress vectorization due to known bug.  See HIVE-19015.
set hive.vectorized.execution.enabled=false;
set hive.test.vectorized.execution.enabled.override=none;

-- this test reads and writes a parquet file with a map of arrays of ints
-- validates PARQUET-26 is fixed

CREATE TABLE parquet_map_of_arrays_of_ints (
    examples MAP<STRING, ARRAY<INT>>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/StringMapOfOptionalIntArray.parquet'
OVERWRITE INTO TABLE parquet_map_of_arrays_of_ints;

CREATE TABLE parquet_map_of_arrays_of_ints_copy STORED AS PARQUET AS SELECT * FROM parquet_map_of_arrays_of_ints;

SELECT * FROM parquet_map_of_arrays_of_ints_copy;

DROP TABLE parquet_map_of_arrays_of_ints;
DROP TABLE parquet_map_of_arrays_of_ints_copy;
