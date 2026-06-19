-- HIVE-25104: Backward incompatible timestamp serialization in Parquet for certain timezones
-- Test writing timestamps in Parquet tables using old and new date/time APIs
-- using the appropriate configuration properties. 
CREATE TABLE employee(eid INT,birth TIMESTAMP) STORED AS PARQUET;
-- Rows written using legacy conversion enabled (1 to 4) are backwards compatible and
-- can be read correctly by older versions of Hive (e.g., Hive 2).
SET hive.parquet.timestamp.write.legacy.conversion.enabled=true;
INSERT INTO employee VALUES (1, '1220-01-01 00:00:00');
INSERT INTO employee VALUES (2, '1880-01-01 00:00:00');
INSERT INTO employee VALUES (3, '1884-01-01 00:00:00');
INSERT INTO employee VALUES (4, '1990-01-01 00:00:00');
-- Rows written using legacy conversion disabled (5 to 8) are not backwards compatible.
-- Reading those rows in older versions of Hive (or other applications) might show
-- the timestamps (5, 6) shifted for some timezones (e.g., US/Pacific).
SET hive.parquet.timestamp.write.legacy.conversion.enabled=false;
INSERT INTO employee VALUES (5, '1220-01-01 00:00:00');
INSERT INTO employee VALUES (6, '1880-01-01 00:00:00');
INSERT INTO employee VALUES (7, '1884-01-01 00:00:00');
INSERT INTO employee VALUES (8, '1990-01-01 00:00:00');
-- No matter how timestamps are written they are always read correctly by the current
-- version of Hive by exploiting the metadata in the file
SELECT eid, birth FROM employee ORDER BY eid;
-- Changing the read property does not have any effect in the current version of Hive
-- since the file metadata contains the appropriate information to read them correctly 
SET hive.parquet.timestamp.legacy.conversion.enabled=false;
SELECT eid, birth FROM employee ORDER BY eid;
SET hive.parquet.timestamp.legacy.conversion.enabled=true;
SELECT eid, birth FROM employee ORDER BY eid;