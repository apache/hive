CREATE TABLE text_tbl (a STRUCT<b:STRUCT<c:INT>>)
STORED AS TEXTFILE;

-- This inserts one NULL row
INSERT OVERWRITE TABLE text_tbl
SELECT IF(false, named_struct("b", named_struct("c", 1)), NULL)
FROM src LIMIT 1;

-- We test that parquet is written with a level 0 definition
CREATE TABLE parq_tbl
STORED AS PARQUET
AS SELECT * FROM text_tbl;

SELECT * FROM text_tbl;
SELECT * FROM parq_tbl;

DROP TABLE text_tbl;
DROP TABLE parq_tbl;