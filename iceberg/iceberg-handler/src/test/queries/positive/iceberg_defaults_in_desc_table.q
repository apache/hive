-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/

CREATE TABLE ice_parq (
  id INT)
STORED BY ICEBERG stored as parquet
TBLPROPERTIES ('format-version'='3');

INSERT INTO ice_parq (id) VALUES (1);

ALTER TABLE ice_parq ADD COLUMNS (point STRUCT<x:INT, y:INT> DEFAULT '{"x":100,"y":99}',
  name STRING DEFAULT 'unknown',
  age INT DEFAULT 25);

INSERT INTO ice_parq (id) VALUES (2);

SELECT * FROM ice_parq ORDER BY id;

DESCRIBE FORMATTED ice_parq;

CREATE TABLE ice_orc (
  id INT)
STORED BY ICEBERG stored as orc
TBLPROPERTIES ('format-version'='3');

INSERT INTO ice_orc (id) VALUES (1);

ALTER TABLE ice_orc ADD COLUMNS (point STRUCT<x:INT, y:INT> DEFAULT '{"x":100,"y":99}',
  name STRING DEFAULT 'unknown',
  age INT DEFAULT 25);

INSERT INTO ice_orc (id) VALUES (2);

SELECT * FROM ice_orc ORDER BY id;

DESCRIBE FORMATTED ice_orc;
