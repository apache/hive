-- this test reads and writes a parquet file with a map of maps

CREATE TABLE parquet_map_of_maps (
    map_of_maps MAP<STRING, MAP<STRING, INT>>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/NestedMap.parquet'
OVERWRITE INTO TABLE parquet_map_of_maps;

CREATE TABLE parquet_map_of_maps_copy STORED AS PARQUET AS SELECT * FROM parquet_map_of_maps;

SELECT * FROM parquet_map_of_maps_copy;

DROP TABLE parquet_map_of_maps;
DROP TABLE parquet_map_of_maps_copy;
