CREATE TABLE shtb_test1_n0(KEY STRING, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;
CREATE TABLE shtb_test2_n0(KEY STRING, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;

EXPLAIN
SHOW TABLES 'shtb_*';

SHOW TABLES 'shtb_*';

EXPLAIN
SHOW TABLES LIKE 'shtb_test1_n0|shtb_test2_n0';

SHOW TABLES LIKE 'shtb_test1_n0|shtb_test2_n0';

-- SHOW TABLES FROM/IN database
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE foo_n4(a INT);
CREATE TABLE bar_n0(a INT);
CREATE TABLE baz(a INT);

-- SHOW TABLES basic syntax tests
USE default;
SHOW TABLES FROM test_db;
SHOW TABLES IN test_db;
SHOW TABLES IN test_db "test*";
SHOW TABLES IN test_db LIKE "nomatch";

-- SHOW TABLE EXTENDED basic syntax tests and wildcard
SHOW TABLE EXTENDED IN test_db LIKE foo_n4;
SHOW TABLE EXTENDED IN test_db LIKE "foo_n4";
SHOW TABLE EXTENDED IN test_db LIKE 'foo_n4';
SHOW TABLE EXTENDED IN test_db LIKE `foo_n4`;
SHOW TABLE EXTENDED IN test_db LIKE 'ba*';
SHOW TABLE EXTENDED IN test_db LIKE "ba*";
SHOW TABLE EXTENDED IN test_db LIKE `ba*`;

-- SHOW TABLES from a database with a name that requires escaping
CREATE DATABASE `database`;
USE `database`;
CREATE TABLE foo_n4(a INT);
USE default;
SHOW TABLES FROM `database` LIKE "foo_n4";
