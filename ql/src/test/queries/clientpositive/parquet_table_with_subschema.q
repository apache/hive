-- Sometimes, the user wants to create a table from just a portion of the file schema;
-- This test makes sure that this scenario works;

DROP TABLE test;

-- Current file schema is: (id int, name string, address struct<number:int,street:string,zip:string>);
-- Creates a table from just a portion of the file schema, including struct elements (test lower/upper case as well)
CREATE TABLE test (Name string, address struct<Zip:string,Street:string>) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/HiveGroup.parquet' OVERWRITE INTO TABLE test;
SELECT * FROM test;

DROP TABLE test;