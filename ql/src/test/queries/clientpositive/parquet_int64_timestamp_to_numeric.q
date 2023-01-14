set hive.parquet.write.int64.timestamp=true;
set hive.parquet.timestamp.time.unit=micros;
CREATE TABLE hive_26658_table (ts TIMESTAMP) STORED AS PARQUET;

INSERT INTO hive_26658_table VALUES ('2022-10-21 15:58:32');
INSERT INTO hive_26658_table VALUES ('1970-01-01 00:00:00.000009');

SELECT * FROM hive_26658_table;

set metastore.disallow.incompatible.col.type.changes=false;
ALTER TABLE hive_26658_table CHANGE ts ts TINYINT;

SELECT * FROM hive_26658_table;

ALTER TABLE hive_26658_table CHANGE ts ts SMALLINT;

SELECT * FROM hive_26658_table;

ALTER TABLE hive_26658_table CHANGE ts ts INT;

SELECT * FROM hive_26658_table;

ALTER TABLE hive_26658_table CHANGE ts ts BIGINT;

SELECT * FROM hive_26658_table;

ALTER TABLE hive_26658_table CHANGE ts ts DOUBLE;

SELECT * FROM hive_26658_table;

ALTER TABLE hive_26658_table CHANGE ts ts FLOAT;

SELECT * FROM hive_26658_table;
    
ALTER TABLE hive_26658_table CHANGE ts ts Decimal;

SELECT * FROM hive_26658_table;
