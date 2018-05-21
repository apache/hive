set hive.vectorized.execution.enabled=false;

-- Sometimes, the user wants to create a table from just a portion of the file schema;
-- This test_n6 makes sure that this scenario works;

DROP TABLE test_n6;

-- Current file schema is: (id int, name string, address struct<number:int,street:string,zip:string>);
-- Creates a table from just a portion of the file schema, including struct elements (test_n6 lower/upper case as well)
CREATE TABLE test_n6 (Name string, address struct<Zip:string,Street:string>) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/HiveGroup.parquet' OVERWRITE INTO TABLE test_n6;
SELECT * FROM test_n6;

DROP TABLE test_n6;