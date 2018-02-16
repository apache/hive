CREATE TABLE shcol_test(KEY STRING, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;

EXPLAIN
SHOW COLUMNS from shcol_test;

SHOW COLUMNS from shcol_test;

-- SHOW COLUMNS
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE foo(col1 INT, col2 INT, col3 INT, cola INT, colb INT, colc INT, a INT, b INT, c INT);

-- SHOW COLUMNS basic syntax tests
USE test_db;
SHOW COLUMNS from foo;
SHOW COLUMNS in foo;
SHOW COLUMNS in foo 'col*';
SHOW COLUMNS in foo "col*";
SHOW COLUMNS from foo 'col*';
SHOW COLUMNS from foo "col*";
SHOW COLUMNS from foo "col1|cola";

-- SHOW COLUMNS from a database with a name that requires escaping
CREATE DATABASE `database`;
USE `database`;
CREATE TABLE foo(col1 INT, col2 INT, col3 INT, cola INT, colb INT, colc INT, a INT, b INT, c INT);
SHOW COLUMNS from foo;
SHOW COLUMNS in foo "col*";

-- Non existing column pattern
SHOW COLUMNS in foo "nomatch*";
SHOW COLUMNS in foo "col+";
SHOW COLUMNS in foo "nomatch";

use default;
SHOW COLUMNS from test_db.foo;
SHOW COLUMNS from foo from test_db;
SHOW COLUMNS from foo from test_db "col*";
SHOW COLUMNS from foo from test_db like 'col*';
