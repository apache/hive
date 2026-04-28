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

CREATE TABLE ice_nested (
  id INT)
STORED BY ICEBERG stored as parquet
TBLPROPERTIES ('format-version'='3');

ALTER TABLE ice_nested ADD COLUMNS (
  nested_struct STRUCT<
    outer_field:STRING, 
    inner_struct:STRUCT<inner_a:INT, inner_b:STRUCT<inner_x:STRING, inner_y:STRING>>
  > DEFAULT '{"outer_field":"test","inner_struct":{"inner_a":42,"inner_b":{"inner_x":"nestedx","inner_y":"nestedy"}}}',
  simple_struct STRUCT<a:INT, b:STRING> DEFAULT '{"a":123,"b":"simple"}'
);

DESCRIBE FORMATTED ice_nested;

CREATE TABLE ice_mixed_defaults (
  id INT)
STORED BY ICEBERG stored as parquet
TBLPROPERTIES ('format-version'='3');

ALTER TABLE ice_mixed_defaults ADD COLUMNS (
  col_with_initial STRING DEFAULT 'initial_value'
);

DESCRIBE FORMATTED ice_mixed_defaults;

ALTER TABLE ice_mixed_defaults CHANGE COLUMN col_with_initial col_without_write STRING DEFAULT null;

DESCRIBE FORMATTED ice_mixed_defaults;
