DROP TABLE orc_create;
DROP TABLE orc_create_complex;
DROP TABLE orc_create_staging;

CREATE TABLE orc_create_staging (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    COLLECTION ITEMS TERMINATED BY ','
    MAP KEYS TERMINATED BY ':';

DESCRIBE FORMATTED orc_create_staging;

CREATE TABLE orc_create (key INT, value STRING)
   PARTITIONED BY (ds string)
   STORED AS ORC;

DESCRIBE FORMATTED orc_create;

DROP TABLE orc_create;

CREATE TABLE orc_create (key INT, value STRING)
   PARTITIONED BY (ds string);

DESCRIBE FORMATTED orc_create;

ALTER TABLE orc_create SET FILEFORMAT ORC;

DESCRIBE FORMATTED orc_create;

DROP TABLE orc_create;

set hive.default.fileformat=orc;

CREATE TABLE orc_create (key INT, value STRING)
   PARTITIONED BY (ds string);

DESCRIBE FORMATTED orc_create;

CREATE TABLE orc_create_complex (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) STORED AS ORC;

DESCRIBE FORMATTED orc_create_complex;

LOAD DATA LOCAL INPATH '../data/files/orc_create.txt' OVERWRITE INTO TABLE orc_create_staging;

SELECT * from orc_create_staging;

INSERT OVERWRITE TABLE orc_create_complex SELECT * FROM orc_create_staging;

SELECT * from orc_create_complex;
SELECT str from orc_create_complex;
SELECT mp from orc_create_complex;
SELECT lst from orc_create_complex;
SELECT strct from orc_create_complex;

DROP TABLE orc_create;
DROP TABLE orc_create_complex;
DROP TABLE orc_create_staging;
