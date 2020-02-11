-- verify that for HMS using serde's UPDATE COLUMNS returns error

CREATE TABLE hmsserdetable (name string) STORED AS PARQUET;
DESCRIBE hmsserdetable;

ALTER TABLE hmsserdetable UPDATE COLUMNS;