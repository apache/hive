-- SORT_QUERY_RESULTS
-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
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
