DROP TABLE orc_create;

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

DROP TABLE orc_create;
