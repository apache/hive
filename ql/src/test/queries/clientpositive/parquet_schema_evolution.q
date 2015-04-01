-- Some tables might have extra columns and struct elements on the schema than the on Parquet schema;
-- This is called 'schema evolution' as the Parquet file is not ready yet for such new columns;
-- Hive should support this schema, and return NULL values instead;

DROP TABLE NewStructField;
DROP TABLE NewStructFieldTable;

CREATE TABLE NewStructField(a struct<a1:map<string,string>, a2:struct<e1:int>>) STORED AS PARQUET;

INSERT OVERWRITE TABLE NewStructField SELECT named_struct('a1', map('k1','v1'), 'a2', named_struct('e1',5)) FROM srcpart LIMIT 5;

DESCRIBE NewStructField;
SELECT * FROM NewStructField;

-- Adds new fields to the struct types
ALTER TABLE NewStructField REPLACE COLUMNS (a struct<a1:map<string,string>, a2:struct<e1:int,e2:string>, a3:int>, b int);

DESCRIBE NewStructField;
SELECT * FROM NewStructField;

-- Makes sure that new parquet tables contain the new struct field
CREATE TABLE NewStructFieldTable STORED AS PARQUET AS SELECT * FROM NewStructField;
DESCRIBE NewStructFieldTable;
SELECT * FROM NewStructFieldTable;

DROP TABLE NewStructField;
DROP TABLE NewStructFieldTable;