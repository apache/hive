SHOW DATABASES;

-- CREATE with comment
CREATE DATABASE test_db COMMENT 'Hive test database';
SHOW DATABASES;

-- CREATE INE already exists
CREATE DATABASE IF NOT EXISTS test_db;
SHOW DATABASES;

-- SHOW DATABASES synonym
SHOW SCHEMAS;

-- DROP
DROP DATABASE test_db;
SHOW DATABASES;

-- CREATE INE doesn't exist
CREATE DATABASE IF NOT EXISTS test_db COMMENT 'Hive test database';
SHOW DATABASES;

-- DROP IE exists
DROP DATABASE IF EXISTS test_db;
SHOW DATABASES;

-- DROP IE doesn't exist
DROP DATABASE IF EXISTS test_db;

-- SHOW
CREATE DATABASE test_db;
SHOW DATABASES;

-- SHOW pattern
SHOW DATABASES LIKE 'test*';

-- SHOW pattern
SHOW DATABASES LIKE '*ef*';


USE test_db;
SHOW DATABASES;

-- CREATE table in non-default DB
CREATE TABLE test_table (col1 STRING) STORED AS TEXTFILE;
SHOW TABLES;

-- DESCRIBE table in non-default DB
DESCRIBE test_table;

-- DESCRIBE EXTENDED in non-default DB
DESCRIBE EXTENDED test_table;

-- CREATE LIKE in non-default DB
CREATE TABLE test_table_like LIKE test_table;
SHOW TABLES;
DESCRIBE EXTENDED test_table_like;

-- LOAD and SELECT
LOAD DATA LOCAL INPATH '../data/files/test.dat' OVERWRITE INTO TABLE test_table ;
SELECT * FROM test_table;

-- DROP and CREATE w/o LOAD
DROP TABLE test_table;
SHOW TABLES;

CREATE TABLE test_table (col1 STRING) STORED AS TEXTFILE;
SHOW TABLES;

SELECT * FROM test_table;

-- CREATE table that already exists in DEFAULT
USE test_db;
CREATE TABLE src (col1 STRING) STORED AS TEXTFILE;
SHOW TABLES;

SELECT * FROM src LIMIT 10;

USE default;
SELECT * FROM src LIMIT 10;

-- DROP DATABASE
USE test_db;

DROP TABLE src;
DROP TABLE test_table;
DROP TABLE test_table_like;
SHOW TABLES;

USE default;
DROP DATABASE test_db;
SHOW DATABASES;
