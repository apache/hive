set hive.cli.print.header=true;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
SET hive.vectorized.execution.enabled=true;

CREATE TABLE orc_create_staging (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    COLLECTION ITEMS TERMINATED BY ','
    MAP KEYS TERMINATED BY ':';

LOAD DATA LOCAL INPATH '../../data/files/orc_create.txt' OVERWRITE INTO TABLE orc_create_staging;

CREATE TABLE orc_create_complex (
  str STRING,
  mp  MAP<STRING,STRING>,
  lst ARRAY<STRING>,
  strct STRUCT<A:STRING,B:STRING>
) STORED AS ORC;

INSERT OVERWRITE TABLE orc_create_complex SELECT * FROM orc_create_staging;

-- Since complex types are not supported, this query should not vectorize.
EXPLAIN
SELECT * FROM orc_create_complex;

SELECT * FROM orc_create_complex;

-- However, since this query is not referencing the complex fields, it should vectorize.
EXPLAIN
SELECT COUNT(*) FROM orc_create_complex;

SELECT COUNT(*) FROM orc_create_complex;

-- Also, since this query is not referencing the complex fields, it should vectorize.
EXPLAIN
SELECT str FROM orc_create_complex ORDER BY str;

SELECT str FROM orc_create_complex ORDER BY str;