-- SORT_QUERY_RESULTS
-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/

-- Create test table with unknown placeholder column
CREATE EXTERNAL TABLE unknown_test_basic (
    id INT,
    placeholder UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='3');

-- Unknown columns are not stored; only NULL values are accepted
INSERT INTO unknown_test_basic VALUES
(1, NULL),
(2, NULL);

SELECT id, placeholder FROM unknown_test_basic ORDER BY id;

-- Add another unknown column to an existing table
ALTER TABLE unknown_test_basic ADD COLUMNS (extra UNKNOWN);

INSERT INTO unknown_test_basic VALUES
(3, NULL, NULL);

SELECT id, placeholder, extra FROM unknown_test_basic ORDER BY id;

-- CTLT preserves unknown column types from the source table
CREATE TABLE unknown_test_ctlt LIKE unknown_test_basic STORED BY ICEBERG tblproperties('format-version'='3');

DESC FORMATTED unknown_test_ctlt;

INSERT INTO unknown_test_ctlt SELECT * FROM unknown_test_basic;

SELECT id, placeholder, extra FROM unknown_test_ctlt ORDER BY id;

-- CTAS preserves unknown column types from the query schema
CREATE TABLE unknown_test_ctas STORED BY ICEBERG tblproperties('format-version'='3') AS SELECT * FROM unknown_test_basic;

DESC FORMATTED unknown_test_ctas;

SELECT id, placeholder, extra FROM unknown_test_ctas ORDER BY id;

-- Partial column insert: omitted UNKNOWN column is implicit NULL
CREATE EXTERNAL TABLE unknown_test_partial (
    name STRING,
    id UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO unknown_test_partial (name) VALUES ('aqc');

SELECT name, id FROM unknown_test_partial;

-- Explicit NULL in a column list
INSERT INTO unknown_test_partial (name, id) VALUES ('explicit', NULL);

-- Multi-row partial column insert
INSERT INTO unknown_test_partial (name) VALUES ('multi1'), ('multi2');

-- Partial column insert from a SELECT
INSERT INTO unknown_test_partial (name) SELECT CAST(id AS STRING) FROM unknown_test_basic;

SELECT name, id FROM unknown_test_partial ORDER BY name;

-- Rewriting an UNKNOWN column with its own values
INSERT OVERWRITE TABLE unknown_test_partial SELECT name, id FROM unknown_test_partial WHERE name LIKE 'multi%';

SELECT name, id FROM unknown_test_partial ORDER BY name;

-- UNKNOWN column between other columns, omitted from the column list
CREATE EXTERNAL TABLE unknown_test_middle (
    a INT,
    b UNKNOWN,
    c INT
) STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO unknown_test_middle (a, c) VALUES (1, 10);
INSERT INTO unknown_test_middle (c, a) VALUES (20, 2);
INSERT INTO unknown_test_middle VALUES (3, NULL, 30);

SELECT a, b, c FROM unknown_test_middle ORDER BY a;

-- Filters and aggregates on an UNKNOWN column
SELECT count(*) FROM unknown_test_middle WHERE b IS NULL;
SELECT count(*) FROM unknown_test_middle WHERE b IS NOT NULL;
SELECT count(b), count(*) FROM unknown_test_middle;

-- GROUP BY, DISTINCT, windowing and join with an UNKNOWN column
SELECT b, count(*) FROM unknown_test_middle GROUP BY b;
SELECT DISTINCT b FROM unknown_test_middle;
SELECT a, b, row_number() OVER (PARTITION BY b ORDER BY a) FROM unknown_test_middle;
SELECT m.a, m.b, p.placeholder FROM unknown_test_middle m JOIN unknown_test_basic p ON m.a = p.id;

-- Row-mode execution and non-converted fetch
set hive.vectorized.execution.enabled=false;
SELECT a, b, c FROM unknown_test_middle ORDER BY a;
SELECT b, count(*) FROM unknown_test_middle GROUP BY b;
set hive.vectorized.execution.enabled=true;

set hive.fetch.task.conversion=none;
SELECT a, b, c FROM unknown_test_middle;
set hive.fetch.task.conversion=more;

-- Views over UNKNOWN columns
CREATE VIEW unknown_test_view AS SELECT a, b FROM unknown_test_middle;
SELECT a, b FROM unknown_test_view ORDER BY a;

-- UPDATE, DELETE and MERGE on a table with an UNKNOWN column
UPDATE unknown_test_middle SET c = 11 WHERE a = 1;
UPDATE unknown_test_middle SET b = NULL WHERE a = 3;
DELETE FROM unknown_test_middle WHERE a = 2;
MERGE INTO unknown_test_middle t
USING (SELECT 3 AS a, 33 AS c UNION ALL SELECT 4 AS a, 40 AS c) s
ON t.a = s.a
WHEN MATCHED THEN UPDATE SET c = s.c
WHEN NOT MATCHED THEN INSERT (a, c) VALUES (s.a, s.c);

SELECT a, b, c FROM unknown_test_middle ORDER BY a;

-- Column statistics on a table with an UNKNOWN column
ANALYZE TABLE unknown_test_middle COMPUTE STATISTICS FOR COLUMNS;

SELECT a, b, c FROM unknown_test_middle ORDER BY a;

-- UNKNOWN field inside a struct
CREATE EXTERNAL TABLE unknown_test_nested (
    id INT,
    s STRUCT<x:INT, u:UNKNOWN>
) STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO unknown_test_nested (id) VALUES (1);
INSERT INTO unknown_test_nested SELECT 2, named_struct('x', 5, 'u', NULL);

SELECT id, s, s.u FROM unknown_test_nested ORDER BY id;

-- NULL is the only default an UNKNOWN column accepts
CREATE EXTERNAL TABLE unknown_test_show (
    id INT,
    u UNKNOWN DEFAULT NULL,
    s STRUCT<x:INT, u:UNKNOWN>
) STORED BY ICEBERG tblproperties('format-version'='3');

SHOW CREATE TABLE unknown_test_show;

INSERT INTO unknown_test_show (id) VALUES (1);

SELECT id, u, s FROM unknown_test_show;

-- ORC and Avro data files
CREATE EXTERNAL TABLE unknown_test_orc (
    a INT,
    b UNKNOWN,
    c INT,
    s STRUCT<x:INT, u:UNKNOWN>
) STORED BY ICEBERG tblproperties('format-version'='3', 'write.format.default'='orc');

INSERT INTO unknown_test_orc (a, c) VALUES (1, 10), (2, 20);
INSERT INTO unknown_test_orc SELECT 3, NULL, 30, named_struct('x', 5, 'u', NULL);
UPDATE unknown_test_orc SET c = 11 WHERE a = 1;
DELETE FROM unknown_test_orc WHERE a = 2;

SELECT a, b, c, s, s.u FROM unknown_test_orc ORDER BY a;
SELECT count(*) FROM unknown_test_orc WHERE b IS NULL AND c > 10;

set hive.vectorized.execution.enabled=false;
SELECT a, b, c, s, s.u FROM unknown_test_orc ORDER BY a;
set hive.vectorized.execution.enabled=true;

CREATE EXTERNAL TABLE unknown_test_avro (
    a INT,
    b UNKNOWN,
    c INT,
    s STRUCT<x:INT, u:UNKNOWN>
) STORED BY ICEBERG tblproperties('format-version'='3', 'write.format.default'='avro');

INSERT INTO unknown_test_avro (a, c) VALUES (1, 10), (2, 20);
INSERT INTO unknown_test_avro SELECT 3, NULL, 30, named_struct('x', 5, 'u', NULL);
UPDATE unknown_test_avro SET c = 11 WHERE a = 1;
DELETE FROM unknown_test_avro WHERE a = 2;

SELECT a, b, c, s, s.u FROM unknown_test_avro ORDER BY a;
SELECT count(*) FROM unknown_test_avro WHERE b IS NULL AND c > 10;

-- Promote an UNKNOWN column to a concrete type; existing rows read as NULL
ALTER TABLE unknown_test_middle CREATE TAG before_promotion;

ALTER TABLE unknown_test_middle CHANGE COLUMN b b INT;

DESCRIBE unknown_test_middle;

INSERT INTO unknown_test_middle VALUES (5, 500, 50);

SELECT a, b, c FROM unknown_test_middle ORDER BY a;

-- A promoted column evolves like any other column afterwards
ALTER TABLE unknown_test_middle CHANGE COLUMN b b BIGINT;

SELECT a, b, c FROM unknown_test_middle ORDER BY a;

-- A snapshot taken before the promotion still reads
SELECT a, b, c FROM unknown_test_middle FOR SYSTEM_VERSION AS OF 'before_promotion' ORDER BY a;

-- Promotion combined with a rename, a comment and a reorder
CREATE EXTERNAL TABLE unknown_test_promote (
    id INT,
    u1 UNKNOWN,
    u2 UNKNOWN,
    u3 UNKNOWN,
    v INT
) STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO unknown_test_promote (id, v) VALUES (1, 10);

ALTER TABLE unknown_test_promote CHANGE COLUMN u1 renamed STRING;
ALTER TABLE unknown_test_promote CHANGE COLUMN u2 u2 INT COMMENT 'promoted';
ALTER TABLE unknown_test_promote CHANGE COLUMN u3 u3 DOUBLE AFTER v;

DESCRIBE unknown_test_promote;

INSERT INTO unknown_test_promote VALUES (2, 'two', 20, 200, 2.5);

SELECT id, renamed, u2, v, u3 FROM unknown_test_promote ORDER BY id;

-- Dropping an UNKNOWN column
ALTER TABLE unknown_test_basic REPLACE COLUMNS (id INT, placeholder UNKNOWN);

DESCRIBE unknown_test_basic;

SELECT * FROM unknown_test_basic ORDER BY id;
