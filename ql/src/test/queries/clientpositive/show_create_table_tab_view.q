-- Test SHOW CREATE TABLE when view is expanded text contains a tab (HIVE-29059).
-- IDE may replace tabs with spaces during edit. 
-- This test qfile requires actual tabs instead of soft tabs/spaces to function properly

DROP VIEW IF EXISTS hive_29059_v;
DROP VIEW IF EXISTS hive_29059_v2;
DROP VIEW IF EXISTS hive_29059_v3;
DROP VIEW IF EXISTS hive_29059_v4;
DROP VIEW IF EXISTS hive_29059_v5;
DROP TABLE IF EXISTS hive_29059_src;
CREATE TABLE hive_29059_src (val1 string COMMENT 'val	1', 
                             val2 string COMMENT 'val
                                                2', 
                             val3 string);
INSERT INTO hive_29059_src VALUES ('a1', 'b1', 'c1');

-- Create view from source table with tab in the WHERE clause
CREATE VIEW hive_29059_v AS SELECT * FROM hive_29059_src 
	WHERE val1	= 'a1'
	AND val2	= 'b1';
SHOW CREATE TABLE hive_29059_v;

-- Make sure show create table for non-view tables don't break
SHOW CREATE TABLE hive_29059_src;

-- Create view with TAB in string literal
CREATE VIEW hive_29059_v2 AS SELECT 'before
	after' AS col1;
SHOW CREATE TABLE hive_29059_v2;

CREATE VIEW hive_29059_v3 AS SELECT '	a		b 	c		' AS col1;
SHOW CREATE TABLE hive_29059_v3;

CREATE VIEW hive_29059_v4 AS SELECT 'a\\tb\tc' AS col1;
SHOW CREATE TABLE hive_29059_v4;

CREATE VIEW hive_29059_v5 AS SELECT val1 FROM hive_29059_src WHERE val1 LIKE 'a%	';
SHOW CREATE TABLE hive_29059_v5;


DROP VIEW IF EXISTS hive_29059_v;
DROP VIEW IF EXISTS hive_29059_v2;
DROP VIEW IF EXISTS hive_29059_v3;
DROP VIEW IF EXISTS hive_29059_v4;
DROP VIEW IF EXISTS hive_29059_v5;
DROP TABLE IF EXISTS hive_29059_src;
