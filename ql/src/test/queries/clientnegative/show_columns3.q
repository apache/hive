CREATE DATABASE test_db;
USE test_db;
CREATE TABLE foo(a INT);

use default;
SHOW COLUMNS from foo like "a*" from test_db;