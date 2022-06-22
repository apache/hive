CREATE TABLE shcol_test(KEY STRING, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;

EXPLAIN
SHOW COLUMNS from shcol_test;

SHOW COLUMNS from shcol_test;

-- SHOW COLUMNS
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE foo_n7(col1 INT, col2 INT, col3 INT, cola INT, colb INT, colc INT, a INT, b INT, c INT);

-- SHOW COLUMNS basic syntax tests
USE test_db;
EXPLAIN SHOW COLUMNS from foo_n7;
SHOW COLUMNS from foo_n7;
EXPLAIN SHOW COLUMNS in foo_n7;
SHOW COLUMNS in foo_n7;
SHOW COLUMNS in foo_n7 'col*';
SHOW COLUMNS in foo_n7 "col*";
SHOW COLUMNS from foo_n7 'col*';
SHOW COLUMNS from foo_n7 "col*";
EXPLAIN SHOW COLUMNS from foo_n7 "col1|cola";
SHOW COLUMNS from foo_n7 "col1|cola";

-- SHOW COLUMNS from a database with a name that requires escaping
CREATE DATABASE `database`;
USE `database`;
CREATE TABLE foo_n7(col1 INT, col2 INT, col3 INT, cola INT, colb INT, colc INT, a INT, b INT, c INT);
SHOW COLUMNS from foo_n7;
SHOW COLUMNS in foo_n7 "col*";

-- Non existing column pattern
SHOW COLUMNS in foo_n7 "nomatch*";
SHOW COLUMNS in foo_n7 "col+";
SHOW COLUMNS in foo_n7 "nomatch";

use default;
EXPLAIN SHOW COLUMNS from test_db.foo_n7;
SHOW COLUMNS from test_db.foo_n7;
SHOW COLUMNS from foo_n7 from test_db;
SHOW COLUMNS from foo_n7 from test_db "col*";
EXPLAIN SHOW COLUMNS from foo_n7 from test_db like 'col*';
SHOW COLUMNS from foo_n7 from test_db like 'col*';

-- SORTED output
SHOW SORTED COLUMNS from test_db.foo_n7;
SHOW SORTED COLUMNS FROM foo_n7 in `database`;
SHOW SORTED COLUMNS in foo_n7 from test_db "col+";
SHOW SORTED COLUMNS in foo_n7 from test_db "c";
SHOW SORTED COLUMNS from foo_n7 from test_db "c*";
SHOW SORTED COLUMNS from foo_n7 from test_db like 'c*';

-- show column for table with chinese comments. 名 UTF code is 0x540D. D means CR ( '\r'). It should not cause issue.
create table tbl_test (fld string COMMENT '期末日期', fld1 string COMMENT '班次名称', fld2  string COMMENT '排班人数');
show columns from tbl_test;